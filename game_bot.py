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

# --- ПОДДЕРЖКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Если python-dotenv не установлен, продолжаем работу (на сервере он обычно есть)
    pass

# --- АВТОМАТИЧЕСКАЯ ПРОВЕРКА ЗАВИСИМОСТЕЙ ---

def check_dependencies():
    """
    Проверяет наличие необходимых библиотек перед инициализацией.
    На Docker-хостингах установка должна происходить через requirements.txt.
    """
    required = ["aiogram", "aiohttp"]
    for package in required:
        try:
            __import__(package)
        except ImportError:
            logging.warning(f"Внимание: Пакет {package} не найден в системном окружении.")

# --- НАСТРОЙКА ГЛУБОКОГО ЛОГИРОВАНИЯ ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("dionysus_runtime.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("Dionysus_Core")

# Вызов проверки зависимостей
check_dependencies()

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

TOKEN = os.getenv("BOT_TOKEN", "8372090739:AAGRq6MymU_fMXrWiFbfZ7lCRMT2BY9Dz0Y") 
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", 1197260250))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", -1003806822122))

if not TOKEN:
    logger.critical("КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не обнаружен. Проверьте переменные окружения.")
    sys.exit(1)

# --- ИНИЦИАЛИЗАЦИЯ БОТА И ДИСПЕТЧЕРА (ДО ОПРЕДЕЛЕНИЯ ФУНКЦИЙ) ---
bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- УПРАВЛЕНИЕ ФАЙЛОВОЙ СИСТЕМОЙ ---

# Путь к папке data (стандарт для Docker)
DATA_DIR = Path("/app/data")
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception as e:
    # Если запуск не в Docker и прав на /app нет, используем локальную папку
    DATA_DIR = Path(__file__).resolve().parent / "data"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# Путь к базе данных
DB_PATH = DATA_DIR / "bot.db"

# --- МАССИВНЫЕ ИГРОВЫЕ БАЗЫ ДАННЫХ (CONTENT PACK) ---

BUNKER_DATA = {
    "professions": [
        "Врач-хирург", "Инженер-атомщик", "Повар молекулярной кухни", "Программист ИИ", 
        "Учитель начальных классов", "Кадровый военный", "Художник-акционист", "Фермер", 
        "Ученый-генетик", "Слесарь 6 разряда", "Пилот гражданской авиации", "Психолог", 
        "Строитель-высотник", "Электрик", "Химик-технолог", "Библиотекарь", "Эколог",
        "Астроном", "Журналист", "Адвокат", "Музыкант", "Священник", "Полицейский",
        "Ветеринар", "Архитектор", "Дизайнер", "Экономист", "Логист", "Переводчик",
        "Моряк дальнего плавания", "Геолог", "Археолог", "Фармацевт", "Космонавт",
        "Ювелир", "Пожарный", "Крановщик", "Лесник", "Криптовалютчик", "Каскадер",
        "Реставратор", "Океанолог", "Таксист", "Охранник", "Дипломат", "Блогер"
    ],
    "health": [
        "Идеальное здоровье", "Хронический кашель", "Слепота на один глаз", "Крепкий иммунитет", 
        "Бессонница", "Астма в легкой форме", "Аллергия на пыль", "Легкая хромота", 
        "Отличное зрение", "Порох в пороховницах", "Диабет 2 типа", "Анемия", 
        "Слабое сердце", "Повышенное давление", "Хорошая физическая форма", "Плоскостопие",
        "Отсутствие фаланги пальца", "Заикание при испуге", "Дальтонизм", "Мигрени",
        "Шум в ушах", "Боязнь темноты", "Лунатизм", "Боли в суставах", "Нервный тик"
    ],
    "traits": [
        "Трудолюбие", "Скрытность", "Лидерские качества", "Паникер", "Оптимист", 
        "Скептик", "Агрессивность", "Альтруизм", "Гениальность", "Медлительность",
        "Внимательность", "Рассеянность", "Хладнокровие", "Честность", "Хитрость",
        "Выносливость", "Перфекционизм", "Авантюризм", "Трусость", "Смелость",
        "Верность", "Эгоизм", "Саркастичность", "Замкнутость", "Миролюбие",
        "Цинизм", "Наивность", "Прагматичность", "Педантичность", "Радикализм"
    ],
    "hobbies": [
        "Игра на гитаре", "Паркур", "Чтение классики", "Садоводство", "Вязание", 
        "Бокс", "Шахматы", "Кулинария", "Рыбалка", "Охота", "Йога", "Стрельба",
        "Фотография", "Коллекционирование ножей", "Реставрация мебели", "Танцы",
        "Астрология", "Оригами", "Битбокс", "Альпинизм", "Пчеловодство",
        "Нумизматика", "Гончарное дело", "Макетирование", "Виноделие", "Фехтование"
    ],
    "baggage": [
        "Охотничий нож", "Армейская аптечка", "Фонарик на солнечных батареях", "Мешок семян", 
        "Книга 'Как выжить'", "Старая фотография семьи", "Рация (радиус 5 км)", "Зажигалка", 
        "Веревка 10 метров", "Компас", "Топор", "Набор рыболова", "Бутылка виски",
        "Газовая горелка", "Карта местности", "Монтировка", "Бинокль", "Плеер с музыкой",
        "Губная гармошка", "Лупа", "Пачка сигарет", "Набор швейных игл", "Мел",
        "Резиновая лодка", "Сачок", "Упаковка антибиотиков", "Свисток", "Металлоискатель"
    ]
}

TESTS_DB = [
    "https://t.me/quizbot?start=test1", 
    "https://t.me/quizbot?start=personality_test", 
    "https://t.me/quizbot?start=who_are_you",
    "https://t.me/quizbot?start=iq_test_mini",
    "https://t.me/quizbot?start=career_path",
    "https://t.me/quizbot?start=logic_puzzle",
    "https://t.me/quizbot?start=fantasy_role"
]

BINGO_DB = [
    "https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Bingo_card.svg/512px-Bingo_card.svg.png",
    "https://raw.githubusercontent.com/otter007/bingo-cards/master/bingo.png",
    "https://i.pinimg.com/originals/9f/8e/4a/9f8e4a9e3e3b3e3e3e3e3e3e3e3e3e3e.png"
]

CROC_WORDS = [
    "Синхрофазотрон", "Бутерброд", "Космонавт", "Прокрастинация", "Электричество", 
    "Зебра", "Мим", "Гироскутер", "Экскаватор", "Телепортация", "Фотосинтез",
    "Эволюция", "Гравитация", "Интуиция", "Параллелепипед", "Адреналин", "Вдохновение",
    "Круговорот", "Метаморфоза", "Скептицизм", "Филантроп", "Дискриминация", "Харизма",
    "Апокалипсис", "Иллюстрация", "Конфронтация", "Оптимизация", "Спецификация",
    "Абстракция", "Деградация", "Коллективизация", "Индустриализация", "Либерализация",
    "Демократия", "Бюрократия", "Манипуляция", "Синхронизация"
]

KMK_CHARACTERS = [
    "Шрек", "Гарри Поттер", "Тони Старк", "Гермиона Грейнджер", "Джокер", "Мастер Йода", 
    "Дарт Вейдер", "Бэтмен", "Капитан Америка", "Чудо-Женщина", "Шерлок Холмс", 
    "Джек Воробей", "Геральт из Ривии", "Лара Крофт", "Наруто", "Питер Пэн",
    "Рик Санчез", "Морти", "Эминем", "Илон Маск", "Джон Уик", "Терминатор",
    "Гена Букин", "Саша Белый", "Человек-паук", "Танос", "Железный человек"
]

TRUTH_DB = [
    "Твой самый неловкий момент в жизни?", "В кого ты был тайно влюблен в школе?", 
    "Что ты скрываешь от родителей даже сейчас?", "Самая большая ложь в твоей жизни?",
    "Какое самое странное блюдо ты ел?", "Твой самый большой страх?", 
    "О чем ты жалеешь больше всего?", "Твое первое впечатление о человеке слева?",
    "Что бы ты изменил в своем прошлом?", "Самый безумный поступок ради денег?",
    "Был ли у тебя воображаемый друг?", "Какая твоя самая вредная привычка?",
    "Ты когда-нибудь подслушивал чужие разговоры?", "Самый нелепый слух о тебе?",
    "Что ты сделаешь, если найдешь миллион долларов?", "Твой самый странный сон?"
]

DARE_DB = [
    "Пришли свое самое смешное селфи прямо сейчас", "Спой припев любой песни в голосовом сообщении", 
    "Напиши бывшему/бывшей 'Привет' и покажи скриншот", "Станцуй под воображаемую музыку на видео (15 сек)",
    "Расскажи анекдот с серьезным лицом", "Позвони другу и скажи, что ты выиграл миллион",
    "Изобрази тюленя в течение 20 секунд", "Сделай 10 приседаний, считая на иностранном языке",
    "Напиши в чат 10 комплиментов админу", "Выпей стакан воды залпом",
    "Пришли фото своего холодильника", "Поставь на аватарку в ТГ на час фото чеснока",
    "Напиши любому человеку 'Я знаю твой секрет'", "Пришли последнее фото из галереи",
    "Расскажи стишок про ДИОНИСА"
]

NHIE_DB = [
    "Я никогда не прыгал с парашютом", "Я никогда не ел пиццу с ананасами", 
    "Я никогда не просыпал работу/учебу", "Я никогда не крал вещи из отелей",
    "Я никогда не пользовался чужой зубной щеткой", "Я никогда не врал о своем возрасте",
    "Я никогда не плакал во время фильма", "Я никогда не засыпал в транспорте",
    "Я никогда не терял ключи от дома", "Я никогда не пробовал корма для животных",
    "Я никогда не разбивал экран телефона", "Я никогда не купался в одежде",
    "Я никогда не удалял историю браузера в панике", "Я никогда не пел в душе"
]

N5_DB = [
    "Назови 5 городов на букву 'А'", "Назови 5 марок горького шоколада", 
    "Назови 5 мужских имен на букву 'К'", "Назови 5 видов морских рыб",
    "Назови 5 стран Африки", "Назови 5 персонажей мультфильмов Disney",
    "Назови 5 предметов в кабинете стоматолога", "Назови 5 языков программирования",
    "Назови 5 компонентов салата Оливье", "Назови 5 столиц Европы",
    "Назови 5 инструментов оркестра", "Назови 5 видов холодного оружия",
    "Назови 5 марок кроссовок", "Назови 5 имен на 'М'"
]

N7_DB = [
    "Назови 7 марок немецких машин", "Назови 7 героев вселенной Marvel", 
    "Назови 7 видов экзотических фруктов", "Назови 7 столиц азиатских стран",
    "Назови 7 предметов бытовой техники", "Назови 7 названий созвездий",
    "Назови 7 цветов радуги (в правильном порядке)", "Назови 7 великих русских поэтов",
    "Назови 7 деталей системного блока ПК", "Назови 7 видов спорта с мячом",
    "Назови 7 химических элементов", "Назови 7 чудес света",
    "Назови 7 фильмов с Томом Крузом", "Назови 7 марок телефонов"
]

PLAYER_EMOJIS = ["🍷", "📺", "👍", "😂", "🎭", "🎮", "🎲", "🌟", "🔥", "🧊", "🌪️", "⚡", "🍀", "🧿", "💎", "🔋", "🔑", "🚀", "🛸", "🌈"]

# --- ШАБЛОН ПОЛЯ МОНОПОЛИИ ---

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

# --- ГЛОБАЛЬНЫЕ СЕССИИ (IN-MEMORY) ---

fortune_system = {}  # {user_id: {"name": str, "emoji": str, "stats": dict}}
mafia_sessions = {}
bunker_sessions = {}
monopoly_sessions = {}
tictactoe_games = {}

# --- СИСТЕМА УПРАВЛЕНИЯ БАЗОЙ ДАННЫХ (SQLITE) ---

def init_db():
    """
    Инициализирует базу данных при старте.
    Автоматически создает файл и таблицы, если они отсутствуют.
    """
    db_exists = DB_PATH.exists()
    
    try:
        # Создаем папку, если она не существует
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # Подключаемся к базе
        with sqlite3.connect(str(DB_PATH)) as conn:
            cur = conn.cursor()
            
            # 1. Обязательная таблица users (согласно вашему запросу)
            cur.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 2. Таблица профилей для игровых модулей
            cur.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    emoji TEXT,
                    xp INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0
                )
            """)
            
            # 3. Таблица кастомного контента
            cur.execute("""
                CREATE TABLE IF NOT EXISTS custom_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT,
                    content TEXT,
                    added_by INTEGER
                )
            """)
            conn.commit()
            
            if not db_exists:
                logger.info(f"СИСТЕМА: База данных создана успешно по пути: {DB_PATH}")
                print(f"База данных создана: {DB_PATH}")
            else:
                logger.info(f"СИСТЕМА: База данных уже существует: {DB_PATH}")
                print(f"База данных уже существует: {DB_PATH}")
                
            # Подгружаем профили в память
            cur.execute("SELECT user_id, name, emoji FROM profiles")
            for row in cur.fetchall():
                fortune_system[row[0]] = {
                    "name": row[1], 
                    "emoji": row[2], 
                    "stats": {"xp": 0, "games": 0}
                }
            
            # Подгружаем кастомный контент
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
                    
        logger.info("СИНХРОНИЗАЦИЯ: Таблицы синхронизированы с памятью бота.")
    except Exception as e:
        logger.error(f"ОШИБКА БД: Сбой инициализации SQLite: {e}", exc_info=True)

def save_profile_db(user_id: int, name: str, emoji: Optional[str]):
    """Обновляет запись о пользователе в базе данных."""
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO profiles (user_id, name, emoji) 
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET 
                    name=excluded.name, 
                    emoji=excluded.emoji
            """, (user_id, name, emoji))
            
            # Также обновляем таблицу users для истории
            cur.execute("INSERT OR IGNORE INTO users (id, username) VALUES (?, ?)", (user_id, name))
            
            conn.commit()
    except Exception as e:
        logger.error(f"DB ERROR: Не удалось сохранить профиль {user_id}: {e}")

def add_custom_item_db(category: str, content: str, user_id: int):
    """Сохраняет новый элемент игры в БД."""
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO custom_items (category, content, added_by) VALUES (?, ?, ?)", 
                (category, content, user_id)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"DB ERROR: Ошибка записи контента: {e}")

# --- MIDDLEWARE: ОГРАНИЧЕНИЕ ПО ГРУППАМ ---

class RestrictChatMiddleware(BaseMiddleware):
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
            # Супер-админ может пользоваться ботом везде
            user_id = data.get("event_from_user").id if data.get("event_from_user") else 0
            if user_id == SUPER_ADMIN_ID:
                return await handler(event, data)
                
            # Разрешаем ЛС или конкретный чат
            if chat.type == "private" or chat.id == ALLOWED_GROUP_ID:
                return await handler(event, data)
            else:
                return
        return await handler(event, data)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async def is_admin(message: types.Message) -> bool:
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
    return any([
        chat_id in mafia_sessions,
        chat_id in bunker_sessions,
        chat_id in monopoly_sessions,
        chat_id in tictactoe_games
    ])

def get_user_profile(user: types.User) -> dict:
    if user.id not in fortune_system:
        fortune_system[user.id] = {
            "name": user.first_name, 
            "emoji": None,
            "stats": {"xp": 0, "games": 0}
        }
        save_profile_db(user.id, user.first_name, None)
    return fortune_system[user.id]

# --- КОМАНДЫ ПРОФИЛЯ ---

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
    for uid, data in fortune_system.items():
        if data.get("emoji") == emoji and uid != message.from_user.id:
            return await message.answer("😂 Данный маркер уже занят другим объектом. Выберите свободный 👍.")
            
    profile = get_user_profile(message.from_user)
    profile["emoji"] = emoji
    save_profile_db(message.from_user.id, profile["name"], emoji)
    await message.answer(f"😂 Визуальный маркер {emoji} успешно привязан к вашему ID 📺.")

# --- МОДУЛЬ: МАФИЯ ---

async def mafia_check_win(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s: return False
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    mafias = [p for p in alive.values() if p["role"] == "Мафия"]
    
    if not mafias:
        await bot.send_message(chat_id, "😂 <b>Симуляция завершена.</b>\nПобеда мирных! 🍷.", parse_mode="HTML")
        del mafia_sessions[chat_id]
        return True
    if len(mafias) >= (len(alive) - len(mafias)):
        await bot.send_message(chat_id, "😂 <b>Симуляция завершена.</b>\nПобеда мафии! 📺.", parse_mode="HTML")
        del mafia_sessions[chat_id]
        return True
    return False

async def mafia_night_cycle(chat_id: int):
    s = mafia_sessions.get(chat_id)
    s["phase"] = "night"
    s["night_actions"] = {"kill": None, "heal": None, "check": None}
    
    await bot.send_message(chat_id, "😂 <b>Ночь.</b> Город спит. Активные роли, жду в ЛС 👍.", parse_mode="HTML")
    
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    for uid, p in alive.items():
        if p["role"] in ["Мафия", "Доктор", "Комиссар"]:
            kb = InlineKeyboardBuilder()
            for tid, tp in alive.items():
                if p["role"] == "Мафия" and tp["role"] == "Мафия": continue
                kb.button(text=tp["name"], callback_data=f"mf_act_{chat_id}_{p['role']}_{tid}")
            
            try:
                await bot.send_message(uid, f"😂 Роль: <b>{p['role']}</b>. Цель 🍷:", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
            except TelegramForbiddenError:
                await bot.send_message(chat_id, f"⚠️ <b>{p['name']}</b> не открыл ЛС боту! Пропуск.")

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
    if s["night_actions"][key]: return await c.answer("Уже выбрано!")
    s["night_actions"][key] = tid
    if role == "Комиссар":
        target = s["players"][tid]
        res = "Мафия" if target["role"] == "Мафия" else "Мирный"
        await c.message.edit_text(f"😂 Проверка: {target['name']} — <b>{res}</b> 📺.", parse_mode="HTML")
    else:
        await c.message.edit_text(f"😂 Цель зафиксирована: {s['players'][tid]['name']} 🍷.")
    needed = [p["role"] for p in s["players"].values() if p["is_alive"] and p["role"] in ["Мафия", "Доктор", "Комиссар"]]
    if all(s["night_actions"][{"Мафия":"kill","Доктор":"heal","Комиссар":"check"}[r]] for r in needed):
        if s.get("timer"): s["timer"].cancel()

async def mafia_day_cycle(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] == "day": return
    s["phase"] = "day"
    acts = s["night_actions"]
    await bot.send_message(chat_id, "😂 <b>День.</b> Результаты ночи... 📺", parse_mode="HTML")
    await asyncio.sleep(2)
    killed = acts["kill"]
    healed = acts["heal"]
    if killed and killed != healed:
        p = s["players"][killed]
        p["is_alive"] = False
        await bot.send_message(chat_id, f"😂 Убит: <b>{p['name']}</b> ({p['role']}) 🍷.", parse_mode="HTML")
    else:
        await bot.send_message(chat_id, "😂 Потерь нет. Все на месте 👍.")
    if await mafia_check_win(chat_id): return
    kb = InlineKeyboardBuilder().button(text="Начать голосование 📺", callback_data=f"mf_skip_{chat_id}")
    await bot.send_message(chat_id, "😂 <b>Обсуждение (3 мин).</b> Найдите Мафию 👍.", reply_markup=kb.as_markup(), parse_mode="HTML")
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
    await bot.send_message(chat_id, "😂 <b>Голосование!</b> Кого исключим? 📺:", reply_markup=kb.adjust(2, 1).as_markup(), parse_mode="HTML")
    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await mafia_resolve_voting(chat_id)

@dp.callback_query(F.data.startswith("mf_v_"))
async def mafia_vote_handler(c: CallbackQuery):
    _, _, cid, tid = c.data.split("_")
    cid, tid, uid = int(cid), int(tid), c.from_user.id
    s = mafia_sessions.get(cid)
    if not s or s["phase"] != "voting": return await c.answer("Закрыто!")
    if uid not in s["players"] or not s["players"][uid]["is_alive"]: return await c.answer("Ошибка!")
    if uid in s["votes"]: return await c.answer("Уже!")
    s["votes"][uid] = tid
    await c.answer(f"Голос принят 🍷.")
    alive_count = len([p for p in s["players"].values() if p["is_alive"]])
    if len(s["votes"]) >= alive_count:
        if s.get("timer"): s["timer"].cancel()

async def mafia_resolve_voting(chat_id: int):
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] != "voting": return
    s["phase"] = "ended"
    if not s["votes"]:
        await bot.send_message(chat_id, "😂 Никто не исключен 👍.")
    else:
        from collections import Counter
        counts = Counter(s["votes"].values())
        max_v = max(counts.values())
        cands = [tid for tid, v in counts.items() if v == max_v]
        target_id = random.choice(cands)
        p = s["players"][target_id]
        p["is_alive"] = False
        await bot.send_message(chat_id, f"😂 Исключен <b>{p['name']}</b> ({p['role']}) 🍷.", parse_mode="HTML")
    if not await mafia_check_win(chat_id):
        await mafia_night_cycle(chat_id)

@dp.message(Command("мафия_старт", prefix="!/"))
async def mafia_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Система занята 👍.")
    mafia_sessions[m.chat.id] = {"players": {}, "active": False, "phase": "lobby"}
    kb = InlineKeyboardBuilder().button(text="Войти 🍷", callback_data="mf_join").button(text="Старт 📺", callback_data="mf_launch")
    await m.answer("😂 <b>МАФИЯ.</b> Подключайтесь... 🍷", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "mf_join")
async def mafia_join_handler(c: CallbackQuery):
    s = mafia_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    p = get_user_profile(c.from_user)
    s["players"][c.from_user.id] = {"name": p["name"], "is_alive": True, "role": "Мирный"}
    await c.answer("ОК!")

@dp.callback_query(F.data == "mf_launch")
async def mafia_launch_handler(c: CallbackQuery):
    cid, s = c.message.chat.id, mafia_sessions.get(c.message.chat.id)
    if not s or len(s["players"]) < 4: return await c.answer("Минимум 4 игрока!", show_alert=True)
    s["active"] = True
    uids = list(s["players"].keys())
    random.shuffle(uids)
    s["players"][uids[0]]["role"], s["players"][uids[1]]["role"], s["players"][uids[2]]["role"] = "Мафия", "Доктор", "Комиссар"
    for uid, p in s["players"].items():
        try: await bot.send_message(uid, f"😂 Роль: <b>{p['role']}</b> 🍷.", parse_mode="HTML")
        except: pass
    await c.message.edit_text("😂 Начали! См. роли в ЛС... 📺")
    await mafia_night_cycle(cid)

# --- МОДУЛЬ: МОНОПОЛИЯ ---

def render_mono_keyboard(s: dict, actions: Optional[List[dict]] = None) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    def get_cell_text(idx):
        cell = s["board"][idx]
        occ = [p["emoji"] for p in s["players"].values() if p["pos"] == idx and not p["is_bankrupt"]]
        text = cell["short"]
        if cell.get("owner"): text += f"({s['players'][cell['owner']]['emoji']})"
        if occ: text += " " + "".join(occ)
        return text
    builder.row(*[InlineKeyboardButton(text=get_cell_text(i), callback_data=f"mono_info_{i}") for i in range(0, 4)])
    builder.row(InlineKeyboardButton(text=get_cell_text(15), callback_data="mono_info_15"), InlineKeyboardButton(text="🏢", callback_data="none"), InlineKeyboardButton(text="🏦", callback_data="none"), InlineKeyboardButton(text=get_cell_text(4), callback_data="mono_info_4"))
    builder.row(InlineKeyboardButton(text=get_cell_text(14), callback_data="mono_info_14"), InlineKeyboardButton(text="💰", callback_data="none"), InlineKeyboardButton(text="⚖️", callback_data="none"), InlineKeyboardButton(text=get_cell_text(5), callback_data="mono_info_5"))
    builder.row(*[InlineKeyboardButton(text=get_cell_text(i), callback_data=f"mono_info_{i}") for i in range(13, 9, -1)])
    if actions:
        for btn in actions: builder.row(InlineKeyboardButton(text=btn["text"], callback_data=btn["callback"]))
    return builder.as_markup()

async def mono_process_turn(chat_id: int):
    s = monopoly_sessions.get(chat_id)
    if not s: return
    active = [uid for uid, p in s["players"].items() if not p["is_bankrupt"]]
    if len(active) == 1:
        w = s["players"][active[0]]
        await bot.send_message(chat_id, f"🏆 Победитель: {w['emoji']} <b>{w['name']}</b>! 👍.", parse_mode="HTML")
        del monopoly_sessions[chat_id]
        return
    uid = s["turn_queue"][s["current_turn_idx"]]
    p = s["players"][uid]
    if p["is_bankrupt"]:
        s["current_turn_idx"] = (s["current_turn_idx"] + 1) % len(s["turn_queue"])
        return await mono_process_turn(chat_id)
    kb = render_mono_keyboard(s, [{"text": "Генерация 🎲", "callback": f"mono_roll_{uid}"}])
    txt = f"😂 Ход: {p['emoji']} <b>{p['name']}</b>\n💰 Капитал: <b>{p['balance']}$</b>"
    if s.get("board_msg_id"):
        try: await bot.edit_message_text(txt, chat_id, s["board_msg_id"], reply_markup=kb, parse_mode="HTML")
        except: s["board_msg_id"] = (await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")).message_id
    else:
        msg = await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
        s["board_msg_id"] = msg.message_id

@dp.callback_query(F.data.startswith("mono_roll_"))
async def mono_roll_handler(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    if c.from_user.id != uid: return await c.answer("Не ваш ход! 📺", show_alert=True)
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s: return
    p = s["players"][uid]
    roll = random.randint(1, 4)
    old_pos = p["pos"]
    p["pos"] = (p["pos"] + roll) % len(s["board"])
    if p["pos"] < old_pos:
        p["balance"] += 200
        await bot.send_message(cid, f"😂 <b>{p['name']}</b> прошел СТАРТ (+200$) 🍷.")
    cell = s["board"][p["pos"]]
    res = f"😂 Выпало {roll}.\n📍 Позиция: <b>{cell['name']}</b>."
    if cell["type"] == "tax":
        tax = cell.get("tax_amount", 100)
        p["balance"] -= tax
        res += f"\n💸 Налог: <b>-{tax}$</b>."
        await bot.send_message(cid, res, parse_mode="HTML"); await mono_check_bankruptcy(cid, s, uid)
    elif cell["type"] == "chance":
        mod = random.choice([100, 200, -50, -150])
        p["balance"] += mod
        res += f"\n✨ Шанс: <b>{'+' if mod>0 else ''}{mod}$</b>."
        await bot.send_message(cid, res, parse_mode="HTML"); await mono_check_bankruptcy(cid, s, uid)
    elif cell["type"] == "prop":
        if cell["owner"] is None:
            if p["balance"] >= cell["price"]:
                await bot.send_message(cid, res, parse_mode="HTML")
                kb = render_mono_keyboard(s, [{"text": f"Купить ({cell['price']}$) 🍷", "callback": f"mono_buy_{uid}_{p['pos']}"}, {"text": "Пас 👍", "callback": f"mono_pass_{uid}"}])
                await bot.edit_message_reply_markup(cid, s["board_msg_id"], reply_markup=kb)
                return
            else:
                res += f"\n📺 Мало денег для покупки."
                await bot.send_message(cid, res, parse_mode="HTML"); await mono_check_bankruptcy(cid, s, uid)
        elif cell["owner"] == uid:
            res += "\n🏠 Своя земля. Отдых... 🍷"
            await bot.send_message(cid, res, parse_mode="HTML"); await mono_check_bankruptcy(cid, s, uid)
        else:
            owner = s["players"][cell["owner"]]
            rent = cell["rent"]
            p["balance"] -= rent; owner["balance"] += rent
            res += f"\n💳 Рента объекту {owner['name']}: <b>{rent}$</b>."
            await bot.send_message(cid, res, parse_mode="HTML"); await mono_check_bankruptcy(cid, s, uid)
    else:
        await bot.send_message(cid, res, parse_mode="HTML"); await mono_check_bankruptcy(cid, s, uid)

@dp.callback_query(F.data.startswith("mono_buy_"))
async def mono_buy_callback(c: CallbackQuery):
    _, _, uid, pos = c.data.split("_"); uid, pos = int(uid), int(pos)
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s or c.from_user.id != uid: return
    p, cell = s["players"][uid], s["board"][pos]
    p["balance"] -= cell["price"]; cell["owner"] = uid
    await bot.send_message(cid, f"😂 <b>{p['name']}</b> купил '{cell['name']}' 🍷.")
    await mono_check_bankruptcy(cid, s, uid)

@dp.callback_query(F.data.startswith("mono_pass_"))
async def mono_pass_callback(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s or c.from_user.id != uid: return
    await bot.send_message(cid, f"😂 Объект <b>{s['players'][uid]['name']}</b> пас 📺.")
    await mono_check_bankruptcy(cid, s, uid)

async def mono_check_bankruptcy(cid: int, s: dict, uid: int):
    p = s["players"][uid]
    if p["balance"] < 0:
        p["is_bankrupt"] = True
        for cell in s["board"]:
            if cell.get("owner") == uid: cell["owner"] = None
        await bot.send_message(cid, f"💀 Объект <b>{p['name']}</b> - банкрот! 📺", parse_mode="HTML")
    s["current_turn_idx"] = (s["current_turn_idx"] + 1) % len(s["turn_queue"])
    await asyncio.sleep(1); await mono_process_turn(cid)

@dp.message(Command("монополия_старт", prefix="!/"))
async def mono_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Занято 👍.")
    monopoly_sessions[m.chat.id] = {"players": {}, "active": False, "board_msg_id": None}
    kb = InlineKeyboardBuilder().button(text="Войти 🍷", callback_data="mono_join").button(text="Запуск 😂", callback_data="mono_launch")
    await m.answer("😂 <b>МОНОПОЛИЯ.</b> Ждем инвесторов 🍷.", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "mono_join")
async def mono_join_handler(c: CallbackQuery):
    s = monopoly_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    p = get_user_profile(c.from_user)
    used = [pl["emoji"] for pl in s["players"].values()]
    emoji = p["emoji"] if p["emoji"] and p["emoji"] not in used else next((e for e in PLAYER_EMOJIS if e not in used), "[+]")
    s["players"][c.from_user.id] = {"name": p["name"], "balance": 1000, "pos": 0, "is_bankrupt": False, "emoji": emoji}
    list_str = "\n".join([f"{pl['emoji']} <b>{pl['name']}</b>" for pl in s["players"].values()])
    await c.message.edit_text(f"😂 Регистрация в Монополию:\n{list_str}", reply_markup=c.message.reply_markup, parse_mode="HTML")

@dp.callback_query(F.data == "mono_launch")
async def mono_launch_handler(c: CallbackQuery):
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s or len(s["players"]) < 2: return await c.answer("Нужно минимум 2 игрока!", show_alert=True)
    s["active"] = True
    s["board"] = [dict(cell) for cell in MONOPOLY_BOARD_TEMPLATE]
    s["turn_queue"] = list(s["players"].keys()); random.shuffle(s["turn_queue"]); s["current_turn_idx"] = 0
    await c.message.edit_text("😂 Рынок открыт! 🍷.")
    await mono_process_turn(cid)

# --- МОДУЛЬ: БУНКЕР ---

@dp.message(Command("бункер_старт", prefix="!/"))
async def bunker_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Занято 👍.")
    bunker_sessions[m.chat.id] = {"players": {}, "active": False, "round": 1}
    kb = InlineKeyboardBuilder().button(text="Войти 🍷", callback_data="bn_join").button(text="Запуск 😂", callback_data="bn_launch")
    await m.answer("😂 <b>БУНКЕР.</b> Авторизация... 🍷", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "bn_join")
async def bunker_join_handler(c: CallbackQuery):
    s = bunker_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    p = get_user_profile(c.from_user)
    s["players"][c.from_user.id] = {"name": p["name"]}
    list_str = "\n".join([f"• <b>{p['name']}</b>" for p in s["players"].values()])
    await c.message.edit_text(f"😂 Регистрация в Бункер:\n{list_str}", reply_markup=c.message.reply_markup, parse_mode="HTML")

@dp.callback_query(F.data == "bn_launch")
async def bunker_launch_handler(c: CallbackQuery):
    cid, s = c.message.chat.id, bunker_sessions.get(c.message.chat.id)
    if not s or len(s["players"]) < 3: return await c.answer("Минимум 3 объекта! 👍", show_alert=True)
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
        s["players"][uid] = {"name": p["name"], "traits": t, "revealed": {k: False for k in t}, "rev_this_round": False}
        try:
            msg = (f"😂 <b>Ваш профиль Бункера:</b>\n\n🧬 Био: {t['bio']}\n🛠 Проф: {t['prof']}\n🩺 Здоровье: {t['health']}\n🎭 Черта: {t['trait']}\n🎸 Хобби: {t['hobby']}\n🎒 Багаж: {t['baggage']}\n\nВскрывайте данные кнопками 📺.")
            await bot.send_message(uid, msg, parse_mode="HTML")
        except: pass
    await c.message.edit_text("😂 Начали! Инфо в ЛС 🍷.")
    await bunker_start_round(cid)

async def bunker_start_round(chat_id: int):
    s = bunker_sessions.get(chat_id); if not s: return
    s["phase"] = "discussion"
    for p in s["players"].values(): p["rev_this_round"] = False
    kb = InlineKeyboardBuilder()
    mapping = {"bio": "🧬 Био", "prof": "🛠 Проф", "health": "🩺 Здоровье", "trait": "🎭 Черта", "hobby": "🎸 Хобби", "baggage": "🎒 Багаж"}
    for k, v in mapping.items(): kb.button(text=v, callback_data=f"bn_reveal_{k}")
    kb.button(text="Исключение 📺", callback_data=f"bn_vnow_{chat_id}")
    await bot.send_message(chat_id, f"😂 <b>Цикл {s['round']}.</b> Обсуждение (5 мин). Вскройте параметр 🍷.", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
    s["timer"] = asyncio.create_task(asyncio.sleep(300))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await bunker_start_voting(chat_id)

@dp.callback_query(F.data.startswith("bn_reveal_"))
async def bunker_reveal_handler(c: CallbackQuery):
    trait = c.data.split("_")[2]; cid, uid = c.message.chat.id, c.from_user.id
    s = bunker_sessions.get(cid); if not s or s["phase"] != "discussion": return await c.answer("Нельзя!")
    p = s["players"].get(uid); if not p or p["rev_this_round"] or p["revealed"][trait]: return await c.answer("Ошибка!", show_alert=True)
    p["revealed"][trait] = True; p["rev_this_round"] = True
    tr_ru = {"bio": "Биологию", "prof": "Профессию", "health": "Здоровье", "trait": "Черту", "hobby": "Хобби", "baggage": "Багаж"}[trait]
    await bot.send_message(cid, f"😂 Объект <b>{p['name']}</b> вскрыл <b>{tr_ru}</b>: {p['traits'][trait]} 👍.")
    await c.answer("ОК! 🍷")

async def bunker_start_voting(chat_id: int):
    s = bunker_sessions.get(chat_id); if not s or s["phase"] == "voting": return
    s["phase"], s["votes"] = "voting", {}
    kb = InlineKeyboardBuilder()
    for uid, p in s["players"].items(): kb.button(text=p["name"], callback_data=f"bn_v_{chat_id}_{uid}")
    await bot.send_message(chat_id, "😂 <b>Таймер вышел!</b> Кто лишний? 📺:", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await bunker_resolve_voting(chat_id)

@dp.callback_query(F.data.startswith("bn_v_"))
async def bunker_vote_handler(c: CallbackQuery):
    _, _, cid, tid = c.data.split("_"); cid, tid, uid = int(cid), int(tid), c.from_user.id
    s = bunker_sessions.get(cid); if not s or s["phase"] != "voting" or uid not in s["players"] or uid in s["votes"]: return
    s["votes"][uid] = tid; await c.answer("Принято 🍷.")
    if len(s["votes"]) >= len(s["players"]):
        if s.get("timer"): s["timer"].cancel()

async def bunker_resolve_voting(chat_id: int):
    s = bunker_sessions.get(chat_id); if not s or s["phase"] != "voting": return
    if s["votes"]:
        from collections import Counter
        counts = Counter(s["votes"].values()); target_id = random.choice([tid for tid, v in counts.items() if v == max(counts.values())])
        name = s["players"].pop(target_id)["name"]
        await bot.send_message(chat_id, f"😂 Объект <b>{name}</b> исключен 👍.")
    else: await bot.send_message(chat_id, "😂 Нет голосов 📺.")
    if len(s["players"]) <= 3:
        res = "😂 <b>Завершено. Выжившие:</b>\n\n"; [res.__add__(f"👤 <b>{p['name']}</b>: {p['traits']['prof']}\n") for p in s["players"].values()]
        await bot.send_message(chat_id, res + "\n🍷 Протокол закрыт.", parse_mode="HTML"); del bunker_sessions[chat_id]
    else: s["round"] += 1; await bunker_start_round(chat_id)

# --- МОДУЛЬ: КРЕСТИКИ-НОЛИКИ ---

@dp.message(Command("крестики_нолики", prefix="!/"))
async def ttt_init_cmd(m: Message):
    if check_game_active(m.chat.id): return await m.answer("😂 Занято 👍.")
    tictactoe_games[m.chat.id] = {"board": [None]*9, "turn": "X", "players": []}
    kb = InlineKeyboardBuilder(); [kb.button(text=" ", callback_data=f"ttt_{i}") for i in range(9)]
    await m.answer("😂 <b>Крестики-нолики.</b> 🍷.", reply_markup=kb.adjust(3).as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("ttt_"))
async def ttt_handler(c: CallbackQuery):
    idx = int(c.data.split("_")[1]); cid, uid = c.message.chat.id, c.from_user.id; g = tictactoe_games.get(cid); if not g: return
    p_ids = [p["id"] for p in g["players"]]; if uid not in p_ids:
        if len(g["players"]) < 2: g["players"].append({"id": uid, "name": c.from_user.first_name})
        else: return await c.answer("Полный зал!")
    curr_idx = 0 if g["turn"] == "X" else 1; if g["players"][curr_idx]["id"] != uid or g["board"][idx]: return await c.answer("Ошибка!")
    g["board"][idx] = g["turn"]; wins = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    winner = next((g["turn"] for w in wins if g["board"][w[0]] == g["board"][w[1]] == g["board"][w[2]] != None), None)
    kb = InlineKeyboardBuilder(); [kb.button(text=g["board"][i] or " ", callback_data=f"ttt_{i}") for i in range(9)]
    if winner: await c.message.edit_text(f"😂 Победил <b>{g['players'][curr_idx]['name']}</b>! 🍷", reply_markup=kb.adjust(3).as_markup()); del tictactoe_games[cid]
    elif None not in g["board"]: await c.message.edit_text(f"😂 Ничья! 👍.", reply_markup=kb.adjust(3).as_markup()); del tictactoe_games[cid]
    else: g["turn"] = "O" if g["turn"] == "X" else "X"; await c.message.edit_text(f"😂 Ход: <b>{g['turn']}</b> 📺.", reply_markup=kb.adjust(3).as_markup())

# --- СЕРВИСНЫЕ КОМАНДЫ ---

@dp.message(Command("старт", "start", "help", prefix="!/"))
async def start_help_cmd(m: Message):
    text = ("🍷 <b>СИСТЕМА ДИОНИС v3.6</b>\n👤 <code>!ник [Имя]</code>, <code>!стикер [Emoji]</code>\n🎮 <code>!мафия_старт</code>, <code>!бункер_старт</code>, <code>!монополия_старт</code>\n🧩 <code>!правда</code>, <code>!действие</code>, <code>!крокодил</code>, <code>!тест</code>, <code>!бинго</code>\n🛠 <code>!добавить_[категория] [текст]</code>")
    await m.answer(text, parse_mode="HTML")

@dp.message(Command("правда", "действие", "яникогдане", "назови5", "назови7", prefix="!/"))
async def games_handler(m: Message, command: CommandObject):
    cmd = command.command; db_map = {"правда": TRUTH_DB, "действие": DARE_DB, "яникогдане": NHIE_DB, "назови5": N5_DB, "назови7": N7_DB}
    if cmd in db_map: await m.answer(f"😂 <b>{cmd.upper()}:</b> {random.choice(db_map[cmd])} 🍷", parse_mode="HTML")

@dp.message(Command("тест", "бинго", "кмк", prefix="!/"))
async def modules_handler(m: Message, command: CommandObject):
    cmd = command.command
    if cmd == "тест": await m.answer(f"😂 Психологический опрос:\n{random.choice(TESTS_DB)} 🍷")
    elif cmd == "бинго": await m.answer_photo(photo=random.choice(BINGO_DB), caption="😂 Бинго сформировано 👍.")
    elif cmd == "кмк": await m.answer(f"😂 KMK: 1.<b>{random.choice(KMK_CHARACTERS)}</b> 2.<b>{random.choice(KMK_CHARACTERS)}</b> 3.<b>{random.choice(KMK_CHARACTERS)}</b> 🍷", parse_mode="HTML")

@dp.message(Command("крокодил", prefix="!/"))
async def croc_cmd(m: Message):
    try: await bot.send_message(m.from_user.id, f"😂 Слово: <b>{random.choice(CROC_WORDS)}</b> 📺."); await m.answer(f"😂 Отправлено объекту <b>{m.from_user.first_name}</b> 🍷.")
    except: await m.answer("😂 Напишите боту в ЛС! 👍.")

@dp.message(Command(F.string.startswith("добавить_"), prefix="!/"))
async def add_item_handler(m: Message, command: CommandObject):
    if not await is_admin(m): return
    cat = command.command.replace("добавить_", ""); if not command.args: return
    add_custom_item_db(cat, command.args, m.from_user.id)
    mem_map = {"test": TESTS_DB, "bingo": BINGO_DB, "croc": CROC_WORDS, "kmk": KMK_CHARACTERS, "truth": TRUTH_DB, "dare": DARE_DB, "nhie": NHIE_DB, "n5": N5_DB, "n7": N7_DB}
    if cat in mem_map: mem_map[cat].append(command.args)
    await m.answer(f"😂 База '{cat}' расширена 📺.")

# --- ФОНОВЫЕ ЗАДАЧИ ---

async def health_check_loop():
    while True:
        try:
            await bot.send_message(SUPER_ADMIN_ID, "🍷 Алгоритм ДИОНИС: Статус 'Работаю'.")
            logger.info("SYSTEM: Уведомление отправлено.")
        except Exception: pass
        await asyncio.sleep(3600)

# --- ЗАПУСК ---

async def on_shutdown_logic():
    logger.info("SHUTDOWN: Завершение работы...")
    await bot.session.close()

async def main():
    init_db()
    dp.message.middleware(RestrictChatMiddleware()); dp.callback_query.middleware(RestrictChatMiddleware())
    asyncio.create_task(health_check_loop())
    logger.info("SYSTEM START: Дионис запущен.")
    try:
        await bot.delete_webhook(drop_pending_updates=True); await dp.start_polling(bot)
    finally: await on_shutdown_logic()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): logger.info("SYSTEM STOP.")