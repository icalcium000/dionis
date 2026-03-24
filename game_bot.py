import os
import logging
import random
import asyncio
import sqlite3
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Awaitable, Callable, Union

# --- АВТОМАТИЧЕСКАЯ ПРОВЕРКА И УСТАНОВКА ЗАВИСИМОСТЕЙ ---

def install_dependencies():
    """
    Автоматическая проверка наличия необходимых библиотек.
    Это гарантирует, что на новом сервере бот запустится без ручной установки pip.
    """
    required = ["aiogram", "aiohttp"]
    for package in required:
        try:
            __import__(package)
        except ImportError:
            logging.info(f"Пакет {package} не найден. Инициирую установку...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# --- НАСТРОЙКА СИСТЕМНОГО ЛОГИРОВАНИЯ ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Логи также будут писаться в файл для отладки на сервере
        logging.FileHandler("bot_runtime.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("Dionysus_Alpha_Core")

# Запуск проверки зависимостей перед импортом aiogram
install_dependencies()

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command, CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.types import (
    Message, 
    CallbackQuery, 
    InlineKeyboardButton, 
    BufferedInputFile,
    ReplyKeyboardMarkup,
    KeyboardButton
)

# --- КОНФИГУРАЦИЯ И БЕЗОПАСНОСТЬ (ENV) ---

# Мы используем значения по умолчанию только если переменные окружения не заданы.
# На сервере рекомендуется прописать их в .env или системные переменные.
TOKEN = os.getenv("BOT_TOKEN", "8372090739:AAGRq6MymU_fMXrWiFbfZ7lCRMT2BY9Dz0Y") 
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", 1197260250))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", -1003806822122))

if not TOKEN:
    logger.critical("ОШИБКА: BOT_TOKEN отсутствует. Работа невозможна.")
    sys.exit(1)

# --- УПРАВЛЕНИЕ ФАЙЛОВОЙ СИСТЕМОЙ ---

BASE_DIR = Path(__file__).resolve().parent
# Если бот запущен в Docker, используем стандартный путь /app/data
if Path("/app").exists() or os.getenv("DOCKER_ENV"):
    DATA_DIR = Path("/app/data")
else:
    DATA_DIR = BASE_DIR / "data"

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "game_bd.db"

# --- РАСШИРЕННЫЕ ИГРОВЫЕ БАЗЫ ДАННЫХ (CONTENT PACK) ---

BUNKER_DATA = {
    "professions": [
        "Врач-хирург", "Инженер-атомщик", "Повар молекулярной кухни", "Программист ИИ", 
        "Учитель начальных классов", "Кадровый военный", "Художник-акционист", "Фермер", 
        "Ученый-генетик", "Слесарь 6 разряда", "Пилот гражданской авиации", "Психолог", 
        "Строитель-высотник", "Электрик", "Химик-технолог", "Библиотекарь", "Эколог",
        "Астроном", "Журналист", "Адвокат", "Музыкант", "Священник", "Полицейский",
        "Ветеринар", "Архитектор", "Дизайнер", "Экономист", "Логист", "Переводчик"
    ],
    "health": [
        "Идеальное здоровье", "Хронический кашель", "Слепота на один глаз", "Крепкий иммунитет", 
        "Бессонница", "Астма в легкой форме", "Аллергия на пыль", "Легкая хромота", 
        "Отличное зрение", "Порох в пороховницах", "Диабет 2 типа", "Анемия", 
        "Слабое сердце", "Повышенное давление", "Хорошая физическая форма", "Плоскостопие"
    ],
    "traits": [
        "Трудолюбие", "Скрытность", "Лидерские качества", "Паникер", "Оптимист", 
        "Скептик", "Агрессивность", "Альтруизм", "Гениальность", "Медлительность",
        "Внимательность", "Рассеянность", "Хладнокровие", "Честность", "Хитрость",
        "Выносливость", "Перфекционизм", "Авантюризм", "Трусость", "Смелость"
    ],
    "hobbies": [
        "Игра на гитаре", "Паркур", "Чтение классики", "Садоводство", "Вязание", 
        "Бокс", "Шахматы", "Кулинария", "Рыбалка", "Охота", "Йога", "Стрельба",
        "Фотография", "Коллекционирование ножей", "Реставрация мебели", "Танцы"
    ],
    "baggage": [
        "Охотничий нож", "Армейская аптечка", "Фонарик на солнечных батареях", "Мешок семян", 
        "Книга 'Как выжить'", "Старая фотография семьи", "Рация (радиус 5 км)", "Зажигалка", 
        "Веревка 10 метров", "Компас", "Топор", "Набор рыболова", "Бутылка виски",
        "Газовая горелка", "Карта местности", "Монтировка", "Бинокль", "Плеер с музыкой"
    ]
}

TESTS_DB = [
    "https://t.me/quizbot?start=test1", 
    "https://t.me/quizbot?start=personality_test", 
    "https://t.me/quizbot?start=who_are_you",
    "https://t.me/quizbot?start=iq_test_mini",
    "https://t.me/quizbot?start=career_path"
]

BINGO_DB = [
    "https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Bingo_card.svg/512px-Bingo_card.svg.png",
    "https://raw.githubusercontent.com/otter007/bingo-cards/master/bingo.png"
]

CROC_WORDS = [
    "Синхрофазотрон", "Бутерброд", "Космонавт", "Прокрастинация", "Электричество", 
    "Зебра", "Мим", "Гироскутер", "Экскаватор", "Телепортация", "Фотосинтез",
    "Эволюция", "Гравитация", "Интуиция", "Параллелепипед", "Адреналин", "Вдохновение",
    "Круговорот", "Метаморфоза", "Скептицизм", "Филантроп", "Дискриминация", "Харизма"
]

KMK_CHARACTERS = [
    "Шрек", "Гарри Поттер", "Тони Старк", "Гермиона Грейнджер", "Джокер", "Мастер Йода", 
    "Дарт Вейдер", "Бэтмен", "Капитан Америка", "Чудо-Женщина", "Шерлок Холмс", 
    "Джек Воробей", "Геральт из Ривии", "Лари Крофт", "Наруто", "Питер Пэн"
]

TRUTH_DB = [
    "Твой самый неловкий момент в жизни?", "В кого ты был тайно влюблен в школе?", 
    "Что ты скрываешь от родителей даже сейчас?", "Самая большая ложь в твоей жизни?",
    "Какое самое странное блюдо ты ел?", "Твой самый большой страх?", 
    "О чем ты жалеешь больше всего?", "Твое первое впечатление о человеке слева?",
    "Что бы ты изменил в своем прошлом?", "Самый безумный поступок ради денег?"
]

DARE_DB = [
    "Пришли свое самое смешное селфи прямо сейчас", "Спой припев любой песни в голосовом сообщении", 
    "Напиши бывшему/бывшей 'Привет' и покажи скриншот", "Станцуй под воображаемую музыку на видео (15 сек)",
    "Расскажи анекдот с серьезным лицом", "Позвони другу и скажи, что ты выиграл миллион",
    "Изобрази тюленя в течение 20 секунд", "Сделай 10 приседаний, считая на иностранном языке",
    "Напиши в чат 10 комплиментов админу", "Выпей стакан воды залпом"
]

NHIE_DB = [
    "Я никогда не прыгал с парашютом", "Я никогда не ел пиццу с ананасами", 
    "Я никогда не просыпал работу/учебу", "Я никогда не крал вещи из отелей",
    "Я никогда не пользовался чужой зубной щеткой", "Я никогда не врал о своем возрасте",
    "Я никогда не плакал во время фильма", "Я никогда не засыпал в транспорте",
    "Я никогда не терял ключи от дома", "Я никогда не пробовал корма для животных"
]

N5_DB = [
    "Назови 5 городов на букву 'А'", "Назови 5 марок горького шоколада", 
    "Назови 5 мужских имен на букву 'К'", "Назови 5 видов морских рыб",
    "Назови 5 стран Африки", "Назови 5 персонажей мультфильмов Disney",
    "Назови 5 предметов в кабинете стоматолога", "Назови 5 языков программирования",
    "Назови 5 компонентов салата Оливье", "Назови 5 столиц Европы"
]

N7_DB = [
    "Назови 7 марок немецких машин", "Назови 7 героев вселенной Marvel", 
    "Назови 7 видов экзотических фруктов", "Назови 7 столиц азиатских стран",
    "Назови 7 предметов бытовой техники", "Назови 7 названий созвездий",
    "Назови 7 цветов радуги (в правильном порядке)", "Назови 7 великих русских поэтов",
    "Назови 7 деталей системного блока ПК", "Назови 7 видов спорта с мячом"
]

PLAYER_EMOJIS = ["🍷", "📺", "👍", "😂", "🎭", "🎮", "🎲", "🌟", "🔥", "🧊", "🌪️", "⚡", "🍀", "🧿"]

# --- ШАБЛОН ПОЛЯ МОНОПОЛИИ (УДЛИНЕННЫЙ) ---

MONOPOLY_BOARD_TEMPLATE = [
    {"type": "start", "name": "СТАРТ (+200)", "short": "СТАРТ"},
    {"type": "prop", "name": "Житная ул.", "short": "Житн.", "price": 60, "rent": 10, "owner": None},
    {"type": "chance", "name": "Шанс", "short": "Шанс"},
    {"type": "prop", "name": "Огарева ул.", "short": "Огар.", "price": 100, "rent": 30, "owner": None},
    {"type": "tax", "name": "Налог (-100)", "short": "Налог", "tax_amount": 100},
    {"type": "prop", "name": "ул. Полянка", "short": "Полян.", "price": 140, "rent": 50, "owner": None},
    {"type": "tax", "name": "Штраф (-150)", "short": "Штраф", "tax_amount": 150},
    {"type": "prop", "name": "Арбат", "short": "Арбат", "price": 200, "rent": 80, "owner": None},
    {"type": "prop", "name": "Ростовская наб.", "short": "Ростов.", "price": 220, "rent": 90, "owner": None},
    {"type": "chance", "name": "Шанс", "short": "Шанс"},
    {"type": "prop", "name": "Рязанский пр.", "short": "Рязан.", "price": 260, "rent": 110, "owner": None},
    {"type": "prop", "name": "Кутузовский пр.", "short": "Кутуз.", "price": 300, "rent": 130, "owner": None},
    {"type": "prop", "name": "Гоголевский б-р", "short": "Гогол.", "price": 320, "rent": 150, "owner": None},
    {"type": "tax", "name": "Элитный налог (-200)", "short": "Налог", "tax_amount": 200},
    {"type": "prop", "name": "Смоленская пл.", "short": "Смолен.", "price": 350, "rent": 175, "owner": None},
    {"type": "prop", "name": "Рублевское ш.", "short": "Рубл.", "price": 400, "rent": 200, "owner": None}
]

# --- ГЛОБАЛЬНЫЕ СТРУКТУРЫ ДАННЫХ (IN-MEMORY) ---

fortune_system = {}  # {user_id: {"name": str, "emoji": str, "stats": dict}}
mafia_sessions = {}
bunker_sessions = {}
monopoly_sessions = {}
tictactoe_games = {}

# --- СИСТЕМА УПРАВЛЕНИЯ БАЗОЙ ДАННЫХ ---

def init_db():
    """Инициализация SQLite и загрузка всех данных."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            # Таблица профилей
            cur.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    emoji TEXT,
                    xp INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0
                )
            """)
            # Таблица кастомного контента
            cur.execute("""
                CREATE TABLE IF NOT EXISTS custom_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT,
                    content TEXT,
                    added_by INTEGER
                )
            """)
            conn.commit()
            
            # Загрузка существующих профилей
            cur.execute("SELECT user_id, name, emoji FROM profiles")
            for row in cur.fetchall():
                fortune_system[row[0]] = {
                    "name": row[1], 
                    "emoji": row[2], 
                    "stats": {"xp": 0, "games": 0}
                }
            
            # Синхронизация списков с БД
            cur.execute("SELECT category, content FROM custom_items")
            db_map = {
                "test": TESTS_DB, "bingo": BINGO_DB, "croc": CROC_WORDS, 
                "kmk": KMK_CHARACTERS, "truth": TRUTH_DB, "dare": DARE_DB, 
                "nhie": NHIE_DB, "n5": N5_DB, "n7": N7_DB
            }
            for row in cur.fetchall():
                cat, content = row[0], row[1]
                if cat in db_map and content not in db_map[cat]:
                    db_map[cat].append(content)
                    
        logger.info("База данных успешно инициализирована и синхронизирована.")
    except Exception as e:
        logger.error(f"Критическая ошибка инициализации БД: {e}", exc_info=True)

def save_profile_db(user_id: int, name: str, emoji: Optional[str]):
    """Сохранение/обновление данных пользователя в БД."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO profiles (user_id, name, emoji) 
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET 
                    name=excluded.name, 
                    emoji=excluded.emoji
            """, (user_id, name, emoji))
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка сохранения профиля {user_id}: {e}")

def add_custom_item_db(category: str, content: str, user_id: int):
    """Добавление нового вопроса/слова в базу данных."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO custom_items (category, content, added_by) VALUES (?, ?, ?)", 
                (category, content, user_id)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка добавления элемента ({category}): {e}")

# --- MIDDLEWARE: ОГРАНИЧЕНИЕ ДОСТУПА ---

class RestrictChatMiddleware(BaseMiddleware):
    """
    Middleware для обеспечения безопасности.
    Бот будет реагировать только в ЛС или в ОДНОЙ конкретной группе.
    Это предотвращает использование бота в чужих чатах.
    """
    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        chat = None
        if isinstance(event, types.Message):
            chat = event.chat
        elif isinstance(event, types.CallbackQuery) and event.message:
            chat = event.message.chat
            
        if chat:
            # Разрешаем ЛС или конкретную группу
            if chat.id == SUPER_ADMIN_ID or chat.type == "private" or chat.id == ALLOWED_GROUP_ID:
                return await handler(event, data)
            else:
                # Игнорируем чужие чаты
                return
        return await handler(event, data)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БОТА ---

async def is_admin(message: types.Message) -> bool:
    """Проверка прав администратора для запуска игр."""
    if message.from_user.id == SUPER_ADMIN_ID:
        return True
    if message.chat.type == "private":
        return True
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return member.status in ["administrator", "creator"]
    except Exception:
        return False

def check_game_active(chat_id: int) -> bool:
    """Проверка, запущен ли какой-либо игровой процесс в чате."""
    return any([
        chat_id in mafia_sessions,
        chat_id in bunker_sessions,
        chat_id in monopoly_sessions,
        chat_id in tictactoe_games
    ])

def get_user_profile(user: types.User) -> dict:
    """Получение или создание профиля пользователя в памяти."""
    if user.id not in fortune_system:
        fortune_system[user.id] = {
            "name": user.first_name, 
            "emoji": None,
            "stats": {"xp": 0, "games": 0}
        }
        save_profile_db(user.id, user.first_name, None)
    return fortune_system[user.id]

# --- МОДУЛЬ: УПРАВЛЕНИЕ ПРОФИЛЕМ ---

@dp.message(Command("ник", "name", prefix="!/"))
async def set_name_cmd(message: Message, command: CommandObject):
    if not command.args:
        return await message.answer(
            "😂 Ошибка алгоритма. Требуются входные данные: <code>!ник [ВашНик]</code> 🍷.", 
            parse_mode="HTML"
        )
    name = command.args.strip()[:20]
    profile = get_user_profile(message.from_user)
    profile["name"] = name
    save_profile_db(message.from_user.id, name, profile["emoji"])
    await message.answer(f"😂 Профиль обновлен. Система приветствует вас, <b>{name}</b> 🍷.", parse_mode="HTML")

@dp.message(Command("стикер", "emoji", prefix="!/"))
async def set_emoji_cmd(message: Message, command: CommandObject):
    if not command.args:
        return await message.answer("😂 Некорректный запрос. Используйте: <code>!стикер 🍷</code> 📺.", parse_mode="HTML")
    
    emoji = command.args.strip().split()[0]
    # Проверка на уникальность эмодзи (опционально)
    for uid, data in fortune_system.items():
        if data.get("emoji") == emoji and uid != message.from_user.id:
            return await message.answer("😂 Данный маркер уже занят другим объектом. Выберите свободный 👍.")
            
    profile = get_user_profile(message.from_user)
    profile["emoji"] = emoji
    save_profile_db(message.from_user.id, profile["name"], emoji)
    await message.answer(f"😂 Визуальный маркер {emoji} успешно привязан к вашему ID 📺.")

# --- ЛОГИКА ИГРЫ: МАФИЯ (РАСШИРЕННАЯ) ---

async def mafia_check_win(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s: return False
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    mafias = [p for p in alive.values() if p["role"] == "Мафия"]
    
    if not mafias:
        await bot.send_message(chat_id, "😂 <b>Симуляция завершена.</b> Победа мирных жителей. Угроза устранена 🍷.", parse_mode="HTML")
        del mafia_sessions[chat_id]
        return True
    if len(mafias) >= (len(alive) - len(mafias)):
        await bot.send_message(chat_id, "😂 <b>Симуляция завершена.</b> Победа мафии. Город захвачен 📺.", parse_mode="HTML")
        del mafia_sessions[chat_id]
        return True
    return False

async def mafia_night_cycle(chat_id: int):
    s = mafia_sessions.get(chat_id)
    s["phase"] = "night"
    s["night_actions"] = {"kill": None, "heal": None, "check": None}
    
    await bot.send_message(chat_id, "😂 <b>Фаза: Ночь.</b> Город засыпает. Просыпаются активные роли. Жду ваших команд в ЛС 👍.", parse_mode="HTML")
    
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    for uid, p in alive.items():
        if p["role"] in ["Мафия", "Доктор", "Комиссар"]:
            kb = InlineKeyboardBuilder()
            for tid, tp in alive.items():
                if p["role"] == "Мафия" and tp["role"] == "Мафия": continue
                kb.button(text=tp["name"], callback_data=f"mf_act_{chat_id}_{p['role']}_{tid}")
            
            try:
                await bot.send_message(uid, f"😂 Ваша роль: <b>{p['role']}</b>. Выберите цель 🍷:", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
            except TelegramForbiddenError:
                await bot.send_message(chat_id, f"⚠️ Объект <b>{p['name']}</b> не открыл ЛС боту! Пропуск хода активной роли.", parse_mode="HTML")

    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await mafia_day_cycle(chat_id)

@dp.callback_query(F.data.startswith("mf_act_"))
async def mafia_callback_handler(c: CallbackQuery):
    _, _, cid, role, tid = c.data.split("_")
    cid, tid = int(cid), int(tid)
    s = mafia_sessions.get(cid)
    if not s or s["phase"] != "night": return
    
    key = {"Мафия": "kill", "Доктор": "heal", "Комиссар": "check"}[role]
    if s["night_actions"][key]: return await c.answer("Выбор уже зафиксирован!")
    
    s["night_actions"][key] = tid
    
    if role == "Комиссар":
        target = s["players"][tid]
        res = "Мафия" if target["role"] == "Мафия" else "Мирный"
        await c.message.edit_text(f"😂 Алгоритм подтверждает: {target['name']} — <b>{res}</b> 📺.", parse_mode="HTML")
    else:
        await c.message.edit_text(f"😂 Действие подтверждено для объекта: {s['players'][tid]['name']} 🍷.")
    
    # Проверка, все ли походили
    needed = [p["role"] for p in s["players"].values() if p["is_alive"] and p["role"] in ["Мафия", "Доктор", "Комиссар"]]
    if all(s["night_actions"][{"Мафия":"kill","Доктор":"heal","Комиссар":"check"}[r]] for r in needed):
        if s.get("timer"): s["timer"].cancel()

async def mafia_day_cycle(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] == "day": return
    s["phase"] = "day"
    acts = s["night_actions"]
    
    await bot.send_message(chat_id, "😂 <b>Фаза: День.</b> Алгоритм обрабатывает ночные аномалии... 📺", parse_mode="HTML")
    await asyncio.sleep(2)
    
    killed_id = acts["kill"]
    healed_id = acts["heal"]
    
    if killed_id and killed_id != healed_id:
        p = s["players"][killed_id]
        p["is_alive"] = False
        await bot.send_message(chat_id, f"😂 Жертва ночи: <b>{p['name']}</b> ({p['role']}). Объект выведен из системы 🍷.", parse_mode="HTML")
    else:
        await bot.send_message(chat_id, "😂 Удивительно, но ночь прошла без потерь. Все объекты активны 👍.")
        
    if await mafia_check_win(chat_id): return
    
    kb = InlineKeyboardBuilder().button(text="Завершить обсуждение 📺", callback_data=f"mf_skip_{chat_id}")
    await bot.send_message(chat_id, "😂 Обсуждение (3 мин). Попытайтесь вычислить ошибку в коде (Мафию) 👍.", reply_markup=kb.as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(180))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await mafia_voting_cycle(chat_id)

async def mafia_voting_cycle(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] == "voting": return
    s["phase"], s["votes"] = "voting", {}
    
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    kb = InlineKeyboardBuilder()
    for uid, p in alive.items():
        kb.button(text=p["name"], callback_data=f"mf_v_{chat_id}_{uid}")
    kb.button(text="Завершить 📺", callback_data=f"mf_vend_{chat_id}")
    
    await bot.send_message(chat_id, "😂 <b>Голосование!</b> Выберите объект для деактивации 📺:", reply_markup=kb.adjust(2, 1).as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await mafia_resolve_voting(chat_id)

@dp.callback_query(F.data.startswith("mf_v_"))
async def mafia_vote_handler(c: CallbackQuery):
    _, _, cid, tid = c.data.split("_")
    cid, tid, uid = int(cid), int(tid), c.from_user.id
    s = mafia_sessions.get(cid)
    
    if not s or s["phase"] != "voting": return await c.answer("Голосование не активно!")
    if uid not in s["players"] or not s["players"][uid]["is_alive"]:
        return await c.answer("Мертвые объекты не голосуют! 😂", show_alert=True)
    if uid in s["votes"]: return await c.answer("Голос уже учтен.")
    
    s["votes"][uid] = tid
    await c.answer(f"Принят голос за {s['players'][tid]['name']} 🍷.")
    
    alive_count = len([p for p in s["players"].values() if p["is_alive"]])
    if len(s["votes"]) >= alive_count:
        if s.get("timer"): s["timer"].cancel()

async def mafia_resolve_voting(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] != "voting": return
    s["phase"] = "ended"
    
    if not s["votes"]:
        await bot.send_message(chat_id, "😂 Система не получила данных. Никто не исключен 👍.")
    else:
        from collections import Counter
        counts = Counter(s["votes"].values())
        max_votes = max(counts.values())
        candidates = [tid for tid, v in counts.items() if v == max_votes]
        target_id = random.choice(candidates)
        p = s["players"][target_id]
        p["is_alive"] = False
        await bot.send_message(chat_id, f"😂 Большинством голосов исключен объект <b>{p['name']}</b>. Его роль: <b>{p['role']}</b> 🍷.", parse_mode="HTML")
        
    if not await mafia_check_win(chat_id):
        await mafia_night_cycle(chat_id)

@dp.message(Command("мафия_старт", prefix="!/"))
async def mafia_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Система занята другим алгоритмом 👍.")
    
    mafia_sessions[m.chat.id] = {"players": {}, "active": False, "phase": "lobby"}
    kb = InlineKeyboardBuilder().button(text="Войти 🍷", callback_data="mf_join").button(text="Запуск 😂", callback_data="mf_launch")
    await m.answer("😂 <b>Инициализация Мафии.</b> Ожидание объектов 🍷.", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "mf_join")
async def mafia_join_handler(c: CallbackQuery):
    s = mafia_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    profile = get_user_profile(c.from_user)
    s["players"][c.from_user.id] = {"name": profile["name"], "is_alive": True, "role": "Мирный"}
    await c.answer("Вы авторизованы в системе!")

@dp.callback_query(F.data == "mf_launch")
async def mafia_launch_handler(c: CallbackQuery):
    cid = c.message.chat.id
    s = mafia_sessions.get(cid)
    if not s or len(s["players"]) < 4: return await c.answer("Требуется минимум 4 игрока! 👍", show_alert=True)
    
    s["active"] = True
    uids = list(s["players"].keys())
    random.shuffle(uids)
    s["players"][uids[0]]["role"] = "Мафия"
    s["players"][uids[1]]["role"] = "Доктор"
    s["players"][uids[2]]["role"] = "Комиссар"
    
    for uid, p in s["players"].items():
        try: await bot.send_message(uid, f"😂 Симуляция началась. Ваша роль: <b>{p['role']}</b> 🍷.", parse_mode="HTML")
        except Exception: pass
        
    await c.message.edit_text("😂 Роли распределены. Город засыпает... 📺")
    await mafia_night_cycle(cid)

# --- ЛОГИКА ИГРЫ: МОНОПОЛИЯ (ПОЛНАЯ СЕТКА) ---

def render_mono_keyboard(s: dict, actions: Optional[List[dict]] = None) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    def get_cell_ui(idx):
        cell = s["board"][idx]
        occupants = [p["emoji"] for p in s["players"].values() if p["pos"] == idx and not p["is_bankrupt"]]
        text = cell["short"]
        if cell.get("owner"):
            owner_p = s["players"][cell["owner"]]
            text += f"({owner_p['emoji']})"
        if occupants:
            text += " " + "".join(occupants)
        return text

    # Отрисовка поля 4x4 кнопками
    # Верхний ряд
    builder.row(*[InlineKeyboardButton(text=get_cell_ui(i), callback_data=f"mono_info_{i}") for i in range(0, 4)])
    # Средние ряды
    builder.row(InlineKeyboardButton(text=get_cell_ui(15), callback_data="mono_info_15"), 
                InlineKeyboardButton(text="🏢", callback_data="none"), 
                InlineKeyboardButton(text="🏦", callback_data="none"), 
                InlineKeyboardButton(text=get_cell_ui(4), callback_data="mono_info_4"))
    builder.row(InlineKeyboardButton(text=get_cell_ui(14), callback_data="mono_info_14"), 
                InlineKeyboardButton(text="💰", callback_data="none"), 
                InlineKeyboardButton(text="⚖️", callback_data="none"), 
                InlineKeyboardButton(text=get_cell_ui(5), callback_data="mono_info_5"))
    # Нижний ряд (в обратном порядке для закольцованности визуальной)
    builder.row(*[InlineKeyboardButton(text=get_cell_ui(i), callback_data=f"mono_info_{i}") for i in range(13, 9, -1)])
    
    if actions:
        for btn in actions:
            builder.row(InlineKeyboardButton(text=btn["text"], callback_data=btn["callback"]))
            
    return builder.as_markup()

async def mono_process_turn(chat_id: int):
    s = monopoly_sessions.get(chat_id)
    if not s: return
    
    # Проверка на победителя
    active = [uid for uid, p in s["players"].items() if not p["is_bankrupt"]]
    if len(active) == 1:
        winner = s["players"][active[0]]
        await bot.send_message(chat_id, f"🏆 <b>Экономическое доминирование подтверждено!</b> Победитель: {winner['emoji']} <b>{winner['name']}</b>! 👍.", parse_mode="HTML")
        del monopoly_sessions[chat_id]
        return

    uid = s["turn_queue"][s["current_turn_idx"]]
    p = s["players"][uid]
    
    if p["is_bankrupt"]:
        s["current_turn_idx"] = (s["current_turn_idx"] + 1) % len(s["turn_queue"])
        return await mono_process_turn(chat_id)
        
    kb = render_mono_keyboard(s, [{"text": "Генерация шага (1-4) 🎲", "callback": f"mono_roll_{uid}"}])
    msg_text = f"😂 Ход объекта: {p['emoji']} <b>{p['name']}</b>\n💰 Баланс: <b>{p['balance']}$</b>"
    
    if s.get("board_msg_id"):
        try: await bot.edit_message_text(msg_text, chat_id, s["board_msg_id"], reply_markup=kb, parse_mode="HTML")
        except Exception: pass
    else:
        msg = await bot.send_message(chat_id, msg_text, reply_markup=kb, parse_mode="HTML")
        s["board_msg_id"] = msg.message_id

@dp.callback_query(F.data.startswith("mono_roll_"))
async def mono_roll_handler(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    if c.from_user.id != uid: return await c.answer("Сбой очереди. Сейчас не ваш ход! 📺", show_alert=True)
    
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s: return
    
    p = s["players"][uid]
    roll = random.randint(1, 4)
    old_pos = p["pos"]
    p["pos"] = (p["pos"] + roll) % len(s["board"])
    
    if p["pos"] < old_pos:
        p["balance"] += 200
        await bot.send_message(cid, f"😂 {p['emoji']} <b>{p['name']}</b> прошел СТАРТ. Начислено 200$ 🍷.", parse_mode="HTML")
        
    cell = s["board"][p["pos"]]
    result_text = f"😂 {p['emoji']} <b>{p['name']}</b> выбросил {roll}.\n📍 Позиция: <b>{cell['name']}</b>."
    
    if cell["type"] == "tax":
        amt = cell.get("tax_amount", 100)
        p["balance"] -= amt
        result_text += f"\n💸 Списание налога: <b>-{amt}$</b>."
        await bot.send_message(cid, result_text, parse_mode="HTML")
        await mono_resolve_bankruptcy(cid, s, uid)
    elif cell["type"] == "chance":
        mod = random.choice([100, 200, -50, -150])
        p["balance"] += mod
        result_text += f"\n✨ Аномалия 'Шанс': <b>{'+' if mod>0 else ''}{mod}$</b>."
        await bot.send_message(cid, result_text, parse_mode="HTML")
        await mono_resolve_bankruptcy(cid, s, uid)
    elif cell["type"] == "prop":
        if cell["owner"] is None:
            if p["balance"] >= cell["price"]:
                await bot.send_message(cid, result_text, parse_mode="HTML")
                kb = render_mono_keyboard(s, [
                    {"text": f"Купить за {cell['price']}$ 🍷", "callback": f"mono_buy_{uid}_{p['pos']}"},
                    {"text": "Пропустить ход 👍", "callback": f"mono_pass_{uid}"}
                ])
                await bot.edit_message_reply_markup(cid, s["board_msg_id"], reply_markup=kb)
                return
            else:
                result_text += f"\n📺 Недостаточно средств для покупки ({cell['price']}$)."
                await bot.send_message(cid, result_text, parse_mode="HTML")
                await mono_resolve_bankruptcy(cid, s, uid)
        elif cell["owner"] == uid:
            result_text += "\n🏠 Вы на своей территории. Релаксация... 🍷"
            await bot.send_message(cid, result_text, parse_mode="HTML")
            await mono_resolve_bankruptcy(cid, s, uid)
        else:
            owner = s["players"][cell["owner"]]
            rent = cell["rent"]
            p["balance"] -= rent
            owner["balance"] += rent
            result_text += f"\n💳 Оплата ренты объекту {owner['emoji']} {owner['name']}: <b>{rent}$</b>."
            await bot.send_message(cid, result_text, parse_mode="HTML")
            await mono_resolve_bankruptcy(cid, s, uid)
    else:
        await bot.send_message(cid, result_text, parse_mode="HTML")
        await mono_resolve_bankruptcy(cid, s, uid)

@dp.callback_query(F.data.startswith("mono_buy_"))
async def mono_buy_callback(c: CallbackQuery):
    _, _, uid, pos = c.data.split("_")
    uid, pos = int(uid), int(pos)
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s or c.from_user.id != uid: return
    
    p = s["players"][uid]
    cell = s["board"][pos]
    p["balance"] -= cell["price"]
    cell["owner"] = uid
    
    await bot.send_message(cid, f"😂 Транзакция успешна. <b>{p['name']}</b> приобрел {cell['name']} 🍷.", parse_mode="HTML")
    await mono_resolve_bankruptcy(cid, s, uid)

@dp.callback_query(F.data.startswith("mono_pass_"))
async def mono_pass_callback(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s or c.from_user.id != uid: return
    
    await bot.send_message(cid, f"😂 Объект <b>{s['players'][uid]['name']}</b> отказался от сделки 📺.", parse_mode="HTML")
    await mono_resolve_bankruptcy(cid, s, uid)

async def mono_resolve_bankruptcy(cid: int, s: dict, uid: int):
    p = s["players"][uid]
    if p["balance"] < 0:
        p["is_bankrupt"] = True
        for cell in s["board"]:
            if cell.get("owner") == uid: cell["owner"] = None
        await bot.send_message(cid, f"💀 Объект {p['emoji']} <b>{p['name']}</b> обанкротился и покидает рынок! 📺", parse_mode="HTML")
    
    s["current_turn_idx"] = (s["current_turn_idx"] + 1) % len(s["turn_queue"])
    await asyncio.sleep(1)
    await mono_process_turn(cid)

@dp.message(Command("монополия_старт", prefix="!/"))
async def mono_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Система занята 👍.")
    
    monopoly_sessions[m.chat.id] = {"players": {}, "active": False, "board_msg_id": None}
    kb = InlineKeyboardBuilder().button(text="Авторизация 🍷", callback_data="mono_join").button(text="Запуск 😂", callback_data="mono_launch")
    await m.answer("😂 <b>Экономическая симуляция 'Монополия'.</b> Ожидание игроков 🍷.", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "mono_join")
async def mono_join_handler(c: CallbackQuery):
    s = monopoly_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    p = get_user_profile(c.from_user)
    
    used_emojis = [pl["emoji"] for pl in s["players"].values()]
    emoji = p["emoji"] if p["emoji"] and p["emoji"] not in used_emojis else next((e for e in PLAYER_EMOJIS if e not in used_emojis), "[+]")
    
    s["players"][c.from_user.id] = {"name": p["name"], "balance": 1000, "pos": 0, "is_bankrupt": False, "emoji": emoji}
    p_list = "\n".join([f"{pl['emoji']} <b>{pl['name']}</b>" for pl in s["players"].values()])
    await c.message.edit_text(f"😂 Регистрация в Монополия:\n{p_list}", reply_markup=c.message.reply_markup, parse_mode="HTML")

@dp.callback_query(F.data == "mono_launch")
async def mono_launch_handler(c: CallbackQuery):
    cid = c.message.chat.id
    s = monopoly_sessions.get(cid)
    if not s or len(s["players"]) < 2: return await c.answer("Нужно минимум 2 игрока!", show_alert=True)
    
    s["active"] = True
    s["board"] = [dict(cell) for cell in MONOPOLY_BOARD_TEMPLATE]
    s["turn_queue"] = list(s["players"].keys())
    random.shuffle(s["turn_queue"])
    s["current_turn_idx"] = 0
    
    await c.message.edit_text("😂 Рынок открыт. Стартовый капитал распределен 🍷.")
    await mono_process_turn(cid)

# --- ЛОГИКА ИГРЫ: БУНКЕР (ПОЛНЫЙ ЦИКЛ) ---

@dp.message(Command("бункер_старт", prefix="!/"))
async def bunker_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Система занята другим алгоритмом 👍.")
    
    bunker_sessions[m.chat.id] = {"players": {}, "active": False, "round": 1}
    kb = InlineKeyboardBuilder().button(text="Войти в базу 🍷", callback_data="bn_join").button(text="Запуск 😂", callback_data="bn_launch")
    await m.answer("😂 <b>Протокол 'БУНКЕР' активирован.</b> Ожидание авторизации объектов 🍷.", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "bn_join")
async def bunker_join_handler(c: CallbackQuery):
    s = bunker_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    profile = get_user_profile(c.from_user)
    s["players"][c.from_user.id] = {"name": profile["name"]}
    p_list = "\n".join([f"• <b>{p['name']}</b>" for p in s["players"].values()])
    await c.message.edit_text(f"😂 Регистрация в Бункер:\n{p_list}", reply_markup=c.message.reply_markup, parse_mode="HTML")

@dp.callback_query(F.data == "bn_launch")
async def bunker_launch_handler(c: CallbackQuery):
    cid = c.message.chat.id
    s = bunker_sessions.get(cid)
    if not s or len(s["players"]) < 3: return await c.answer("Требуется минимум 3 объекта! 👍", show_alert=True)
    
    s["active"] = True
    for uid, p in s["players"].items():
        t = {
            "bio": f"{random.randint(18, 80)} лет, {random.choice(['Мужчина', 'Женщина'])}",
            "prof": random.choice(BUNKER_DATA["professions"]),
            "health": random.choice(BUNKER_DATA["health"]),
            "trait": random.choice(BUNKER_DATA["traits"]),
            "hobby": random.choice(BUNKER_DATA["hobbies"]),
            "baggage": random.choice(BUNKER_DATA["baggage"])
        }
        s["players"][uid] = {
            "name": p["name"], "traits": t, 
            "revealed": {k: False for k in t}, "rev_this_round": False
        }
        try:
            msg = (f"😂 <b>Ваши параметры выживания:</b>\n\n"
                   f"🧬 Биология: {t['bio']}\n"
                   f"🛠 Профессия: {t['prof']}\n"
                   f"🩺 Здоровье: {t['health']}\n"
                   f"🎭 Черта: {t['trait']}\n"
                   f"🎸 Хобби: {t['hobby']}\n"
                   f"🎒 Багаж: {t['baggage']}\n\n"
                   f"Используйте кнопки в чате, чтобы вскрыть данные 📺.")
            await bot.send_message(uid, msg, parse_mode="HTML")
        except Exception: pass
        
    await c.message.edit_text("😂 Симуляция запущена. Инструкции отправлены в ЛС 🍷.")
    await bunker_start_round(cid)

async def bunker_start_round(chat_id: int):
    s = bunker_sessions.get(chat_id)
    if not s: return
    s["phase"] = "discussion"
    for p in s["players"].values(): p["rev_this_round"] = False
    
    kb = InlineKeyboardBuilder()
    mapping = {"bio": "Биология", "prof": "Профессия", "health": "Здоровье", "trait": "Черта", "hobby": "Хобби", "baggage": "Багаж"}
    for k, v in mapping.items():
        kb.button(text=v, callback_data=f"bn_reveal_{k}")
    kb.button(text="К голосованию 📺", callback_data=f"bn_vnow_{chat_id}")
    
    await bot.send_message(chat_id, f"😂 <b>Цикл {s['round']}.</b> Фаза обсуждения (5 мин). Вскройте один параметр 🍷.", reply_markup=kb.adjust(3, 3, 1).as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(300))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await bunker_start_voting(chat_id)

@dp.callback_query(F.data.startswith("bn_reveal_"))
async def bunker_reveal_handler(c: CallbackQuery):
    trait = c.data.split("_")[2]
    cid, uid = c.message.chat.id, c.from_user.id
    s = bunker_sessions.get(cid)
    if not s or s["phase"] != "discussion": return await c.answer("Сейчас нельзя вскрывать данные!")
    
    p = s["players"].get(uid)
    if not p: return await c.answer("Вы не в игре!", show_alert=True)
    if p["rev_this_round"]: return await c.answer("Лимит исчерпан в этом цикле! 😂", show_alert=True)
    if p["revealed"][trait]: return await c.answer("Этот параметр уже в общем доступе.")
    
    p["revealed"][trait] = True
    p["rev_this_round"] = True
    val = p["traits"][trait]
    
    trait_ru = {"bio": "Биологию", "prof": "Профессию", "health": "Здоровье", "trait": "Черту", "hobby": "Хобби", "baggage": "Багаж"}[trait]
    await bot.send_message(cid, f"😂 Объект <b>{p['name']}</b> открывает <b>{trait_ru}</b>: {val} 👍.", parse_mode="HTML")
    await c.answer("Данные успешно синхронизированы! 🍷")

async def bunker_start_voting(chat_id: int):
    s = bunker_sessions.get(chat_id)
    if not s or s["phase"] == "voting": return
    s["phase"], s["votes"] = "voting", {}
    
    kb = InlineKeyboardBuilder()
    for uid, p in s["players"].items():
        kb.button(text=p["name"], callback_data=f"bn_v_{chat_id}_{uid}")
    kb.button(text="Сброс таймера 👍", callback_data=f"bn_vend_{chat_id}")
        
    await bot.send_message(chat_id, "😂 <b>Таймер истек!</b> Голосование за исключение лишнего объекта 📺:", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await bunker_resolve_voting(chat_id)

@dp.callback_query(F.data.startswith("bn_v_"))
async def bunker_vote_handler(c: CallbackQuery):
    _, _, cid, tid = c.data.split("_")
    cid, tid, uid = int(cid), int(tid), c.from_user.id
    s = bunker_sessions.get(cid)
    
    if not s or s["phase"] != "voting": return
    if uid not in s["players"]: return await c.answer("Ваш доступ заблокирован!")
    if uid in s["votes"]: return await c.answer("Вы уже проголосовали.")
    
    s["votes"][uid] = tid
    await c.answer(f"Голос против {s['players'][tid]['name']} принят 🍷.")
    
    if len(s["votes"]) >= len(s["players"]):
        if s.get("timer"): s["timer"].cancel()

async def bunker_resolve_voting(chat_id: int):
    s = bunker_sessions.get(chat_id)
    if not s or s["phase"] != "voting": return
    
    if s["votes"]:
        from collections import Counter
        counts = Counter(s["votes"].values())
        target_id = random.choice([tid for tid, v in counts.items() if v == max(counts.values())])
        name = s["players"].pop(target_id)["name"]
        await bot.send_message(chat_id, f"😂 Объект <b>{name}</b> исключен по решению большинства. Люк задраен 👍.", parse_mode="HTML")
    else:
        await bot.send_message(chat_id, "😂 Голоса не поступили. Все остаются на борту 📺.")
        
    if len(s["players"]) <= 3:
        res = "😂 <b>Симуляция завершена. Выжившие:</b>\n\n"
        for p in s["players"].values():
            res += f"👤 <b>{p['name']}</b>: {p['traits']['prof']}, {p['traits']['health']}\n"
        await bot.send_message(chat_id, res + "\n🍷 Протокол закрыт.", parse_mode="HTML")
        del bunker_sessions[chat_id]
    else:
        s["round"] += 1
        await bunker_start_round(chat_id)

@dp.callback_query(F.data.startswith("bn_vnow_"))
async def bunker_skip_discussion(c: CallbackQuery):
    cid = int(c.data.split("_")[2])
    if not await is_admin(c.message): return
    s = bunker_sessions.get(cid)
    if s and s["phase"] == "discussion":
        if s.get("timer"): s["timer"].cancel()
    await c.answer()

@dp.callback_query(F.data.startswith("bn_vend_"))
async def bunker_end_vote_early(c: CallbackQuery):
    cid = int(c.data.split("_")[2])
    if not await is_admin(c.message): return
    s = bunker_sessions.get(cid)
    if s and s["phase"] == "voting":
        if s.get("timer"): s["timer"].cancel()
    await c.answer()

# --- МОДУЛЬ: КРЕСТИКИ-НОЛИКИ ---

@dp.message(Command("крестики_нолики", "ttt", prefix="!/"))
async def ttt_init(m: Message):
    if check_game_active(m.chat.id): return await m.answer("😂 Система занята 👍.")
    tictactoe_games[m.chat.id] = {"board": [None]*9, "turn": "X", "players": []}
    
    kb = InlineKeyboardBuilder()
    for i in range(9): kb.button(text=" ", callback_data=f"ttt_{i}")
    await m.answer("😂 <b>Крестики-нолики.</b> Жду двух игроков 🍷.", reply_markup=kb.adjust(3).as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("ttt_"))
async def ttt_handler(c: CallbackQuery):
    idx = int(c.data.split("_")[1])
    cid, uid = c.message.chat.id, c.from_user.id
    g = tictactoe_games.get(cid)
    if not g: return
    
    # Авторизация игроков
    if uid not in [p["id"] for p in g["players"]]:
        if len(g["players"]) < 2:
            g["players"].append({"id": uid, "name": c.from_user.first_name})
        else: return await c.answer("Мест нет!")
        
    # Ход
    curr_idx = 0 if g["turn"] == "X" else 1
    if g["players"][curr_idx]["id"] != uid: return await c.answer("Не ваш ход! 📺", show_alert=True)
    if g["board"][idx]: return await c.answer("Занято!")
    
    g["board"][idx] = g["turn"]
    
    # Проверка победы
    wins = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    winner = None
    for w in wins:
        if g["board"][w[0]] == g["board"][w[1]] == g["board"][w[2]] != None:
            winner = g["turn"]
            
    kb = InlineKeyboardBuilder()
    for i in range(9): kb.button(text=g["board"][i] or " ", callback_data=f"ttt_{i}")
    kb.adjust(3)
    
    p1 = g["players"][0]["name"]
    p2 = g["players"][1]["name"] if len(g["players"]) > 1 else "???"
    
    if winner:
        await c.message.edit_text(f"😂 <b>Победил {g['players'][curr_idx]['name']}!</b> 🍷", reply_markup=kb.as_markup(), parse_mode="HTML")
        del tictactoe_games[cid]
    elif None not in g["board"]:
        await c.message.edit_text(f"😂 <b>Ничья!</b> Система перезагружена 👍.", reply_markup=kb.as_markup(), parse_mode="HTML")
        del tictactoe_games[cid]
    else:
        g["turn"] = "O" if g["turn"] == "X" else "X"
        await c.message.edit_text(f"😂 {p1} (X) vs {p2} (O). Ход: <b>{g['turn']}</b> 📺.", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- СЕРВИСНЫЕ И АДМИНИСТРАТИВНЫЕ КОМАНДЫ ---

@dp.message(Command("старт", "start", "help", prefix="!/"))
async def start_help_cmd(m: Message):
    text = (
        "🍷 <b>Приветствую в системе ДИОНИС v3.0</b>\n"
        "Я — развлекательный алгоритм с глубоким обучением. Твой бокал уже полон.\n\n"
        "👤 <b>Профиль:</b>\n"
        "<code>!ник [Имя]</code> — сменить позывной\n"
        "<code>!стикер [Emoji]</code> — выбрать маркер\n\n"
        "🎮 <b>Групповые симуляции:</b>\n"
        "<code>!мафия_старт</code> — запуск процесса 'Мафия'\n"
        "<code>!бункер_старт</code> — запуск протокола 'Бункер'\n"
        "<code>!монополия_старт</code> — экономический цикл\n"
        "<code>!крестики_нолики</code> — логический дуэль\n\n"
        "🧩 <b>Мини-модули:</b>\n"
        "<code>!правда</code>, <code>!действие</code>, <code>!яникогдане</code>\n"
        "<code>!назови5</code>, <code>!назови7</code>, <code>!крокодил</code>\n"
        "<code>!кмк</code>, <code>!тест</code>, <code>!бинго</code>\n\n"
        "🛠 <b>База данных (Админ):</b>\n"
        "<code>!добавить_[категория] [текст]</code>"
    )
    await m.answer(text, parse_mode="HTML")

@dp.message(Command("правда", "действие", "яникогдане", "назови5", "назови7", prefix="!/"))
async def common_games_handler(m: Message, command: CommandObject):
    cmd = command.command
    db_map = {
        "правда": (TRUTH_DB, "Правда"), "действие": (DARE_DB, "Действие"),
        "яникогдане": (NHIE_DB, "Я никогда не"), "назови5": (N5_DB, "Задача (5)"),
        "назови7": (N7_DB, "Задача (7)")
    }
    db, label = db_map.get(cmd, (None, ""))
    if db:
        await m.answer(f"😂 <b>{label}:</b> {random.choice(db)} 🍷", parse_mode="HTML")

@dp.message(Command("тест", "test", "бинго", "bingo", "кмк", "kmk", prefix="!/"))
async def special_modules_handler(m: Message, command: CommandObject):
    cmd = command.command
    if cmd in ["тест", "test"]:
        if TESTS_DB:
            await m.answer(f"😂 Алгоритм сгенерировал тест:\n{random.choice(TESTS_DB)} 🍷", parse_mode="HTML")
    elif cmd in ["бинго", "bingo"]:
        if BINGO_DB:
            await m.answer_photo(photo=random.choice(BINGO_DB), caption="😂 Матрица вероятностей 'Бинго' сформирована 👍.")
    elif cmd in ["кмк", "kmk"]:
        if len(KMK_CHARACTERS) >= 3:
            s = random.sample(KMK_CHARACTERS, 3)
            await m.answer(f"😂 Матрица выбора KMK:\n1. <b>{s[0]}</b>\n2. <b>{s[1]}</b>\n3. <b>{s[2]}</b> 🍷", parse_mode="HTML")

@dp.message(Command("крокодил", prefix="!/"))
async def croc_cmd(m: Message):
    word = random.choice(CROC_WORDS)
    try:
        await bot.send_message(m.from_user.id, f"😂 Твое секретное слово: <b>{word}</b>. Не показывай никому! 📺", parse_mode="HTML")
        await m.answer(f"😂 Слово успешно передано объекту <b>{m.from_user.first_name}</b> 🍷.")
    except Exception:
        await m.answer("😂 Ошибка! Сначала напиши мне в ЛС, чтобы я мог отправить слово 👍.")

@dp.message(Command(F.string.startswith("добавить_"), prefix="!/"))
async def add_item_handler(m: Message, command: CommandObject):
    if not await is_admin(m): return
    cat = command.command.replace("добавить_", "")
    if not command.args: return await m.answer("😂 Пустой контент. Введите текст через пробел.")
    
    add_custom_item_db(cat, command.args, m.from_user.id)
    # Обновляем в памяти
    mem_map = {"test": TESTS_DB, "bingo": BINGO_DB, "croc": CROC_WORDS, "kmk": KMK_CHARACTERS, "truth": TRUTH_DB, "dare": DARE_DB, "nhie": NHIE_DB, "n5": N5_DB, "n7": N7_DB}
    if cat in mem_map: mem_map[cat].append(command.args)
    await m.answer(f"😂 База данных '{cat}' успешно расширена. Алгоритм обучается 📺.")

# --- ЗАПУСК И ЗАВЕРШЕНИЕ ---

async def on_shutdown_logic(dp: Dispatcher):
    """Корректное закрытие сессий при остановке сервера."""
    logger.info("Инициировано завершение работы бота. Закрытие сессий...")
    await bot.session.close()

async def main():
    # Инициализация ресурсов
    init_db()
    
    # Регистрация middleware
    dp.message.middleware(RestrictChatMiddleware())
    dp.callback_query.middleware(RestrictChatMiddleware())
    
    # Регистрация событий выключения
    dp.shutdown.register(on_shutdown_logic)
    
    logger.info("Алгоритм ДИОНИС запущен и готов к работе в режиме сервера.")
    
    try:
        # Удаляем вебхуки, если они были, и запускаем опрос
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка во время исполнения основного цикла: {e}", exc_info=True)
    finally:
        await on_shutdown_logic(dp)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен пользователем или системой.")