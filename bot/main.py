# bot/main.py
# ============================================================
# 🧠 Telegram бот для автоматического подбора программы концерта
# (оптимизирован для работы на Koyeb)
# ============================================================

import os, sys, json, math, time, threading, requests, multiprocessing
from queue import Queue
from pathlib import Path
from datetime import datetime
from loguru import logger
from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

from utils.docx_reader import read_program
from utils.validator import generate_program_variants, request_stop
from utils.docx_writer import save_program_to_docx
from utils.telegram_utils import send_message, send_document

# ------------------------------------------------------------
# ЛОГИРОВАНИЕ И HEALTH-CHECK
# ------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logger.add(
    "logs/bot_{time:YYYYMMDD}.log",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    level="DEBUG",
)

app_health = Flask(__name__)

@app_health.route("/")
def root():
    return "OK"

@app_health.route("/health")
def health():
    return {"status": "healthy"}, 200

def start_health_server():
    def run():
        app_health.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
    threading.Thread(target=run, daemon=True).start()
    logger.info("💓 Health-check сервер запущен на порту 8000")

# ------------------------------------------------------------
# KEEP-ALIVE
# ------------------------------------------------------------
def start_keep_alive():
    url = os.getenv("KOYEB_APP_URL")
    if not url:
        logger.warning("⚠️ KOYEB_APP_URL не задан, keep-alive отключён")
        return
    def loop():
        while True:
            try:
                requests.get(url, timeout=10)
                logger.debug(f"[keep-alive] Пинг {url} успешен")
            except Exception as e:
                logger.warning(f"[keep-alive] Ошибка keep-alive: {e}")
            time.sleep(240)
    threading.Thread(target=loop, daemon=True).start()
    logger.info(f"🩵 Keep-alive активирован (ping → {url})")

# ------------------------------------------------------------
# TOKEN
# ------------------------------------------------------------
TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    logger.error("❌ Не найден TELEGRAM_TOKEN (или BOT_TOKEN)")
    sys.exit(1)

# ------------------------------------------------------------
# Путь к списку актёров (для стабильной загрузки в Koyeb)
# ------------------------------------------------------------
ACTORS_PATH = Path(__file__).resolve().parent / "actors_list.json"
os.environ["ACTORS_PATH"] = str(ACTORS_PATH)

# ------------------------------------------------------------
# STOP
# ------------------------------------------------------------
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.warning(f"🛑 Пользователь @{user.username} запросил остановку расчёта")
    request_stop()
    await update.message.reply_text(
        "📨 Получен сигнал на остановку. Расчёт будет завершён — ожидайте итоговый вариант..."
    )

# ------------------------------------------------------------
# START
# ------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"👋 Пользователь @{user.username} запустил бота.")
    await update.message.reply_text(
        "👋 Привет! Отправь мне .docx с программой концерта — я её проанализирую и переставлю номера.\n\n"
        "🛑 Командой /stop можно прервать процесс и получить лучший найденный вариант."
    )

# ------------------------------------------------------------
# ПРОГРЕСС-МОНТОР
# ------------------------------------------------------------
def progress_notifier(chat_id, stop_flag):
    logger.info(f"🔔 Прогресс-монитор запущен для chat_id={chat_id}")
    while not stop_flag.is_set():
        time.sleep(180)  # уведомление раз в 3 минуты, чтобы избежать flood limit
        if stop_flag.is_set():
            break
        try:
            send_message(chat_id, "⏳ Расчёт продолжается... бот всё ещё подбирает варианты.")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить статус пользователю {chat_id}: {e}")
    logger.info(f"🛑 Монитор завершён для chat_id={chat_id}")

# ------------------------------------------------------------
# ОГРАНИЧЕНИЕ ОДНОВРЕМЕННЫХ ЗАДАЧ
# ------------------------------------------------------------
TASK_QUEUE = Queue(maxsize=3)

# ------------------------------------------------------------
# ОСНОВНАЯ ГЕНЕРАЦИЯ
# ------------------------------------------------------------
def run_generation(data, document, user_id, username, timestamp):
    try:
        start_time = time.time()
        stop_flag = threading.Event()
        threading.Thread(target=progress_notifier, args=(user_id, stop_flag), daemon=True).start()

        logger.info(f"📦 Запуск generate_program_variants() для @{username}")
        variants, stats = generate_program_variants(data, chat_id=user_id)
        stop_flag.set()

        elapsed_sec = time.time() - start_time
        elapsed = f"{int(elapsed_sec // 60)} мин {int(elapsed_sec % 60)} сек"
        logger.info(f"✅ Расчёт завершён для @{username}, время: {elapsed}")

        if not variants:
            send_message(user_id, "❌ Вариантов программы не нашлось. Попробуйте ещё раз!")
            return

        result = variants[0]
        result_json_path = Path(f"data/result_{timestamp}_{user_id}.json")
        with open(result_json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        logger.info(f"📤 JSON сохранён: {result_json_path}")

        out_path = Path(f"data/output_{timestamp}_{user_id}.docx")
        out_path = Path(save_program_to_docx(result, out_path, original_filename=document.file_name))
        logger.info(f"📄 DOCX сгенерирован: {out_path}")

        final_conf = stats.get("final_conflicts", 0) or 0
        msg = (
            f"🎬 Программа собрана!\n"
            f"🕓 Время: {elapsed}\n"
            f"Проверено перестановок: {stats.get('checked_variants', 0)}\n"
            f"Исходных конфликтов: {stats.get('initial_conflicts', 0)}\n"
            f"Оставшиеся слабые конфликты (до тянучек): {final_conf}\n"
            f"Добавлено тянучек: {stats.get('tyanuchki_added', 0)}"
        )

        tyan_titles = [x["title"] for x in result if x.get("type") == "тянучка"]
        if tyan_titles:
            msg += "\n\n🧩 Тянучки:\n" + "\n".join(f"• {t}" for t in tyan_titles)
        else:
            msg += "\n\n✅ Без тянучек!"

        send_message(user_id, "📤 Отправляю итоговые документы... ⏳")
        send_document(user_id, str(result_json_path), "📗 Итоговая программа (JSON):")
        send_document(user_id, str(out_path), msg)
        send_message(user_id, f"✅ Готово! Время: {elapsed}")
        logger.info(f"📨 Итоговые файлы отправлены пользователю @{username}")

    except Exception as e:
        logger.exception(f"Ошибка генерации для @{username}: {e}")
        try:
            send_message(user_id, f"❌ Ошибка: {e}")
        except Exception as e2:
            logger.error(f"Не удалось уведомить пользователя об ошибке: {e2}")
    finally:
        # освобождаем слот в очереди
        if "TASK_QUEUE" in globals():
            TASK_QUEUE.get()
            TASK_QUEUE.task_done()

# ------------------------------------------------------------
# ОБРАБОТКА ДОКУМЕНТОВ
# ------------------------------------------------------------
async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username or "unknown"
    document = update.message.document

    logger.info(f"📥 Получен файл {document.file_name} от @{username}")
    if not document.file_name.lower().endswith(".docx"):
        await update.message.reply_text("⚠️ Отправь файл в формате .docx.")
        logger.warning(f"⚠️ @{username} отправил неподдерживаемый файл: {document.file_name}")
        return

    if TASK_QUEUE.full():
        await update.message.reply_text("⚠️ Сервер сейчас занят, попробуй позже.")
        logger.warning(f"⚠️ Пропуск задачи: очередь переполнена (пользователь @{username})")
        return
    TASK_QUEUE.put(1)

    file = await document.get_file()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = Path(f"data/{timestamp}__{document.file_name}")
    await file.download_to_drive(local_path)
    logger.info(f"📂 Файл сохранён локально: {local_path}")

    data = read_program(local_path)
    parsed_json_path = Path(f"data/parsed_{timestamp}_{user.id}.json")
    with open(parsed_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # безопасное закрытие файла
    with open(parsed_json_path, "rb") as f:
        await update.message.reply_document(f, caption="📘 Исходные данные после парсинга:")
    logger.info(f"📄 JSON отправлен пользователю @{username}: {parsed_json_path}")

    movable = [i for i, x in enumerate(data)
               if x.get("type") == "обычный" and 2 < i < len(data) - 2]
    count = len(movable)
    factorial_display = str(math.factorial(count)) if count <= 10 else f"≈ {math.factorial(10):.2e}+"
    msg = (
        f"📦 Файл получен!\n"
        f"Количество номеров для перестановки — {count}.\n"
        f"Придётся пересчитать {factorial_display} вариантов.\n"
        f"💪 Подготовка данных, ожидайте запуска перебора!\n\n"
        f"🛑 Можно остановить командой /stop"
    )
    await update.message.reply_text(msg)
    logger.info(f"📊 Начинается расчёт {count}! вариантов для @{username}")

    # запуск расчёта в отдельном процессе
    process = multiprocessing.Process(
        target=run_generation,
        args=(data, document, user.id, username, timestamp),
        daemon=True,
    )
    process.start()
    logger.info(f"🚀 Подпроцесс генерации запущен (pid={process.pid}) для @{username}")

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    logger.info("🚀 Запуск Telegram-бота...")
    start_health_server()
    start_keep_alive()
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))
    logger.info("✅ Хэндлеры загружены. Бот готов к приёму документов.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    main()
