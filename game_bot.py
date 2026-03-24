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
    Проверяет наличие необходимых библиотек перед запуском.
    Это гарантирует работу на сервере сразу после загрузки файла.
    """
    required = ["aiogram", "aiohttp"]
    for package in required:
        try:
            __import__(package)
        except ImportError:
            logging.info(f"Пакет {package} не найден. Начинаю установку...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

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

# Вызов установки перед основным импортом
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

# Все ключи и ID вынесены в переменные окружения.
# Если они не заданы в системе, используются ваши стандартные значения.
TOKEN = os.getenv("BOT_TOKEN", "8372090739:AAGRq6MymU_fMXrWiFbfZ7lCRMT2BY9Dz0Y") 
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", 1197260250))
ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", -1003806822122))

if not TOKEN:
    logger.critical("КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не обнаружен. Работа прекращена.")
    sys.exit(1)

# --- УПРАВЛЕНИЕ ФАЙЛОВОЙ СИСТЕМОЙ ---

BASE_DIR = Path(__file__).resolve().parent
# Путь /app/data используется для Docker-контейнеров
if Path("/app").exists() or os.getenv("DOCKER_MODE"):
    DATA_DIR = Path("/app/data")
else:
    DATA_DIR = BASE_DIR / "data"

# Создаем папку для БД, если её нет
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "game_bd.db"

# --- МАССИВНЫЕ ИГРОВЫЕ БАЗЫ ДАННЫХ (CONTENT PACK) ---

BUNKER_DATA = {
    "professions": [
        "Врач-хирург", "Инженер-атомщик", "Повар молекулярной кухни", "Программист ИИ", 
        "Учитель начальных классов", "Кадровый военный", "Художник-акционист", "Фермер", 
        "Ученый-генетик", "Слесарь 6 разряда", "Пилот гражданской авиации", "Психолог", 
        "Строитель-высотник", "Электрик", "Химик-технолог", "Библиотекарь", "Эколог",
        "Астроном", "Журналист", "Адвокат", "Музыкант", "Священник", "Полицейский",
        "Ветеринар", "Архитектор", "Дизайнер", "Экономист", "Логист", "Переводчик",
        "Моряк дальнего плавания", "Геолог", "Археолог", "Фармацевт", "Космонавт"
    ],
    "health": [
        "Идеальное здоровье", "Хронический кашель", "Слепота на один глаз", "Крепкий иммунитет", 
        "Бессонница", "Астма в легкой форме", "Аллергия на пыль", "Легкая хромота", 
        "Отличное зрение", "Порох в пороховницах", "Диабет 2 типа", "Анемия", 
        "Слабое сердце", "Повышенное давление", "Хорошая физическая форма", "Плоскостопие",
        "Отсутствие фаланги пальца", "Заикание при испуге", "Дальтонизм", "Мигрени"
    ],
    "traits": [
        "Трудолюбие", "Скрытность", "Лидерские качества", "Паникер", "Оптимист", 
        "Скептик", "Агрессивность", "Альтруизм", "Гениальность", "Медлительность",
        "Внимательность", "Рассеянность", "Хладнокровие", "Честность", "Хитрость",
        "Выносливость", "Перфекционизм", "Авантюризм", "Трусость", "Смелость",
        "Верность", "Эгоизм", "Саркастичность", "Замкнутость", "Миролюбие"
    ],
    "hobbies": [
        "Игра на гитаре", "Паркур", "Чтение классики", "Садоводство", "Вязание", 
        "Бокс", "Шахматы", "Кулинария", "Рыбалка", "Охота", "Йога", "Стрельба",
        "Фотография", "Коллекционирование ножей", "Реставрация мебели", "Танцы",
        "Астрология", "Оригами", "Битбокс", "Альпинизм", "Пчеловодство"
    ],
    "baggage": [
        "Охотничий нож", "Армейская аптечка", "Фонарик на солнечных батареях", "Мешок семян", 
        "Книга 'Как выжить'", "Старая фотография семьи", "Рация (радиус 5 км)", "Зажигалка", 
        "Веревка 10 метров", "Компас", "Топор", "Набор рыболова", "Бутылка виски",
        "Газовая горелка", "Карта местности", "Монтировка", "Бинокль", "Плеер с музыкой",
        "Губная гармошка", "Лупа", "Пачка сигарет", "Набор швейных игл", "Мел"
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
    "Апокалипсис", "Иллюстрация", "Конфронтация", "Оптимизация", "Спецификация"
]

KMK_CHARACTERS = [
    "Шрек", "Гарри Поттер", "Тони Старк", "Гермиона Грейнджер", "Джокер", "Мастер Йода", 
    "Дарт Вейдер", "Бэтмен", "Капитан Америка", "Чудо-Женщина", "Шерлок Холмс", 
    "Джек Воробей", "Геральт из Ривии", "Лара Крофт", "Наруто", "Питер Пэн",
    "Рик Санчез", "Морти", "Эминем", "Илон Маск", "Джон Уик", "Терминатор"
]

TRUTH_DB = [
    "Твой самый неловкий момент в жизни?", "В кого ты был тайно влюблен в школе?", 
    "Что ты скрываешь от родителей даже сейчас?", "Самая большая ложь в твоей жизни?",
    "Какое самое странное блюдо ты ел?", "Твой самый большой страх?", 
    "О чем ты жалеешь больше всего?", "Твое первое впечатление о человеке слева?",
    "Что бы ты изменил в своем прошлом?", "Самый безумный поступок ради денег?",
    "Был ли у тебя воображаемый друг?", "Какая твоя самая вредная привычка?",
    "Ты когда-нибудь подслушивал чужие разговоры?"
]

DARE_DB = [
    "Пришли свое самое смешное селфи прямо сейчас", "Спой припев любой песни в голосовом сообщении", 
    "Напиши бывшему/бывшей 'Привет' и покажи скриншот", "Станцуй под воображаемую музыку на видео (15 сек)",
    "Расскажи анекдот с серьезным лицом", "Позвони другу и скажи, что ты выиграл миллион",
    "Изобрази тюленя в течение 20 секунд", "Сделай 10 приседаний, считая на иностранном языке",
    "Напиши в чат 10 комплиментов админу", "Выпей стакан воды залпом",
    "Пришли фото своего холодильника", "Поставь на аватарку в ТГ на час фото чеснока",
    "Напиши любому человеку 'Я знаю твой секрет'"
]

NHIE_DB = [
    "Я никогда не прыгал с парашютом", "Я никогда не ел пиццу с ананасами", 
    "Я никогда не просыпал работу/учебу", "Я никогда не крал вещи из отелей",
    "Я никогда не пользовался чужой зубной щеткой", "Я никогда не врал о своем возрасте",
    "Я никогда не плакал во время фильма", "Я никогда не засыпал в транспорте",
    "Я никогда не терял ключи от дома", "Я никогда не пробовал корма для животных",
    "Я никогда не разбивал экран телефона", "Я никогда не купался в одежде"
]

N5_DB = [
    "Назови 5 городов на букву 'А'", "Назови 5 марок горького шоколада", 
    "Назови 5 мужских имен на букву 'К'", "Назови 5 видов морских рыб",
    "Назови 5 стран Африки", "Назови 5 персонажей мультфильмов Disney",
    "Назови 5 предметов в кабинете стоматолога", "Назови 5 языков программирования",
    "Назови 5 компонентов салата Оливье", "Назови 5 столиц Европы",
    "Назови 5 инструментов оркестра", "Назови 5 видов холодного оружия"
]

N7_DB = [
    "Назови 7 марок немецких машин", "Назови 7 героев вселенной Marvel", 
    "Назови 7 видов экзотических фруктов", "Назови 7 столиц азиатских стран",
    "Назови 7 предметов бытовой техники", "Назови 7 названий созвездий",
    "Назови 7 цветов радуги (в правильном порядке)", "Назови 7 великих русских поэтов",
    "Назови 7 деталей системного блока ПК", "Назови 7 видов спорта с мячом",
    "Назови 7 химических элементов", "Назови 7 чудес света"
]

PLAYER_EMOJIS = ["🍷", "📺", "👍", "😂", "🎭", "🎮", "🎲", "🌟", "🔥", "🧊", "🌪️", "⚡", "🍀", "🧿", "💎", "🔋"]

# --- ШАБЛОН ПОЛЯ МОНОПОЛИИ (ПОЛНЫЙ ЦИКЛ) ---

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
    Инициализирует структуру БД и подгружает данные в память бота.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            # Таблица профилей пользователей
            cur.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    emoji TEXT,
                    xp INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0
                )
            """)
            # Таблица пользовательского игрового контента
            cur.execute("""
                CREATE TABLE IF NOT EXISTS custom_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT,
                    content TEXT,
                    added_by INTEGER
                )
            """)
            conn.commit()
            
            # Загружаем существующие профили
            cur.execute("SELECT user_id, name, emoji FROM profiles")
            for row in cur.fetchall():
                fortune_system[row[0]] = {
                    "name": row[1], 
                    "emoji": row[2], 
                    "stats": {"xp": 0, "games": 0}
                }
            
            # Загружаем кастомные вопросы/слова
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
                    
        logger.info("СИНХРОНИЗАЦИЯ: База данных успешно подключена и обработана.")
    except Exception as e:
        logger.error(f"ОШИБКА БД: Не удалось инициализировать SQLite: {e}", exc_info=True)

def save_profile_db(user_id: int, name: str, emoji: Optional[str]):
    """Обновляет запись о пользователе в базе данных."""
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
            logger.info(f"DB: Профиль {user_id} обновлен (Имя: {name}).")
    except Exception as e:
        logger.error(f"DB ERROR: Не удалось сохранить профиль {user_id}: {e}")

def add_custom_item_db(category: str, content: str, user_id: int):
    """Сохраняет новый элемент игры в БД."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO custom_items (category, content, added_by) VALUES (?, ?, ?)", 
                (category, content, user_id)
            )
            conn.commit()
            logger.info(f"DB: Добавлен новый контент в категорию '{category}'.")
    except Exception as e:
        logger.error(f"DB ERROR: Ошибка записи контента: {e}")

# --- MIDDLEWARE: ОГРАНИЧЕНИЕ ПО ГРУППАМ ---

class RestrictChatMiddleware(BaseMiddleware):
    """
    Обеспечивает безопасность сервера. 
    Бот игнорирует сообщения из любых групп, кроме одной разрешенной.
    Личные сообщения (ЛС) всегда разрешены.
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
            # Супер-админ может пользоваться ботом везде (для тестов)
            user_id = data.get("event_from_user").id if data.get("event_from_user") else 0
            if user_id == SUPER_ADMIN_ID:
                return await handler(event, data)
                
            # Разрешаем ЛС или конкретный чат
            if chat.type == "private" or chat.id == ALLOWED_GROUP_ID:
                return await handler(event, data)
            else:
                return # Игнорируем остальные запросы
        return await handler(event, data)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async def is_admin(message: types.Message) -> bool:
    """Проверяет наличие прав администратора чата или бота."""
    if message.from_user.id == SUPER_ADMIN_ID:
        return True
    if message.chat.type == "private":
        return True
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return member.status in ["administrator", "creator"]
    except Exception:
        return False

def get_user_profile(user: types.User) -> dict:
    """Возвращает данные профиля из памяти, создавая его при отсутствии."""
    if user.id not in fortune_system:
        fortune_system[user.id] = {
            "name": user.first_name, 
            "emoji": None,
            "stats": {"xp": 0, "games": 0}
        }
        save_profile_db(user.id, user.first_name, None)
    return fortune_system[user.id]

# --- МОДУЛЬ: МАФИЯ (ДЕТАЛЬНАЯ ЛОГИКА) ---

async def mafia_check_win(chat_id: int):
    """Анализирует состояние игры и объявляет победителей."""
    s = mafia_sessions.get(chat_id)
    if not s: return False
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    mafias = [p for p in alive.values() if p["role"] == "Мафия"]
    
    if not mafias:
        await bot.send_message(chat_id, "😂 <b>Симуляция завершена.</b>\nПобеда мирных жителей! Угроза устранена 🍷.", parse_mode="HTML")
        del mafia_sessions[chat_id]
        return True
    if len(mafias) >= (len(alive) - len(mafias)):
        await bot.send_message(chat_id, "😂 <b>Симуляция завершена.</b>\nПобеда мафии! Город захвачен преступностью 📺.", parse_mode="HTML")
        del mafia_sessions[chat_id]
        return True
    return False

async def mafia_night_cycle(chat_id: int):
    """Инициирует ночную фазу действий активных ролей."""
    s = mafia_sessions[chat_id]
    s["phase"] = "night"
    s["night_actions"] = {"kill": None, "heal": None, "check": None}
    
    logger.info(f"MAFIA: Начало ночи в чате {chat_id}")
    await bot.send_message(chat_id, "😂 <b>Фаза: Ночь.</b> Город засыпает.\nАктивные роли, жду ваших сигналов в личных сообщениях 👍.", parse_mode="HTML")
    
    alive = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    for uid, p in alive.items():
        if p["role"] in ["Мафия", "Доктор", "Комиссар"]:
            kb = InlineKeyboardBuilder()
            for tid, tp in alive.items():
                if p["role"] == "Мафия" and tp["role"] == "Мафия": continue
                kb.button(text=tp["name"], callback_data=f"mf_act_{chat_id}_{p['role']}_{tid}")
            
            try:
                await bot.send_message(uid, f"😂 Ваша активная роль: <b>{p['role']}</b>.\nСделайте свой выбор 🍷:", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
            except TelegramForbiddenError:
                await bot.send_message(chat_id, f"⚠️ Критическая ошибка: Объект <b>{p['name']}</b> ({p['role']}) не разрешил боту писать в ЛС! Пропуск хода.")

    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: 
        await s["timer"]
    except asyncio.CancelledError: 
        pass
    await mafia_day_cycle(chat_id)

@dp.callback_query(F.data.startswith("mf_act_"))
async def mafia_callback_handler(c: CallbackQuery):
    """Обрабатывает тайные нажатия кнопок в ЛС."""
    _, _, cid, role, tid = c.data.split("_")
    cid, tid = int(cid), int(tid)
    s = mafia_sessions.get(cid)
    if not s or s["phase"] != "night": return
    
    act_key = {"Мафия": "kill", "Доктор": "heal", "Комиссар": "check"}[role]
    if s["night_actions"][act_key]: 
        return await c.answer("Ваше решение уже внесено в базу!")
    
    s["night_actions"][act_key] = tid
    
    if role == "Комиссар":
        target = s["players"][tid]
        res = "Мафия" if target["role"] == "Мафия" else "Мирный"
        await c.message.edit_text(f"😂 Алгоритм проверил объект: {target['name']} — <b>{res}</b> 📺.", parse_mode="HTML")
    else:
        await c.message.edit_text(f"😂 Выбор подтвержден. Цель зафиксирована: {s['players'][tid]['name']} 🍷.")
    
    # Автоматический переход к дню, если все активные роли походили
    needed = [p["role"] for p in s["players"].values() if p["is_alive"] and p["role"] in ["Мафия", "Доктор", "Комиссар"]]
    if all(s["night_actions"][{"Мафия":"kill","Доктор":"heal","Комиссар":"check"}[r]] for r in needed):
        if s.get("timer"): s["timer"].cancel()

async def mafia_day_cycle(chat_id: int):
    """Подводит итоги ночи и запускает обсуждение."""
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] == "day": return
    s["phase"] = "day"
    acts = s["night_actions"]
    
    await bot.send_message(chat_id, "😂 <b>Фаза: День.</b> Расчет ночных событий завершен... 📺", parse_mode="HTML")
    await asyncio.sleep(2)
    
    target_id = acts["kill"]
    healed_id = acts["heal"]
    
    if target_id and target_id != healed_id:
        p = s["players"][target_id]
        p["is_alive"] = False
        await bot.send_message(chat_id, f"😂 Ночная сводка: Объект <b>{p['name']}</b> ({p['role']}) был устранен 🍷.", parse_mode="HTML")
    else:
        await bot.send_message(chat_id, "😂 Ночная сводка: Потерь нет. Все системы защиты сработали штатно 👍.")
        
    if await mafia_check_win(chat_id): return
    
    kb = InlineKeyboardBuilder().button(text="Начать голосование 📺", callback_data=f"mf_skip_{chat_id}")
    await bot.send_message(chat_id, "😂 <b>Обсуждение (3 мин).</b> Вычислите Мафию среди присутствующих 👍.", reply_markup=kb.as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(180))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await mafia_voting_cycle(chat_id)

async def mafia_voting_cycle(chat_id: int):
    """Запускает открытое голосование в чате."""
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] == "voting": return
    s["phase"], s["votes"] = "voting", {}
    
    alive_players = {uid: p for uid, p in s["players"].items() if p["is_alive"]}
    kb = InlineKeyboardBuilder()
    for uid, p in alive_players.items():
        kb.button(text=p["name"], callback_data=f"mf_v_{chat_id}_{uid}")
    kb.button(text="Завершить принудительно 📺", callback_data=f"mf_vend_{chat_id}")
    
    await bot.send_message(chat_id, "😂 <b>Голосование!</b>\nВыберите объект для деактивации 📺:", reply_markup=kb.adjust(2, 1).as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(60))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await mafia_resolve_voting(chat_id)

@dp.callback_query(F.data.startswith("mf_v_"))
async def mafia_vote_handler(c: CallbackQuery):
    """Фиксирует голоса игроков."""
    _, _, cid, tid = c.data.split("_")
    cid, tid, uid = int(cid), int(tid), c.from_user.id
    s = mafia_sessions.get(cid)
    
    if not s or s["phase"] != "voting": return await c.answer("Голосование в данный момент закрыто!")
    if uid not in s["players"] or not s["players"][uid]["is_alive"]:
        return await c.answer("Вы выбыли из симуляции и не можете голосовать!", show_alert=True)
    if uid in s["votes"]: 
        return await c.answer("Ваш голос уже зарегистрирован.")
    
    s["votes"][uid] = tid
    await c.answer(f"Голос против {s['players'][tid]['name']} принят 🍷.")
    
    alive_count = len([p for p in s["players"].values() if p["is_alive"]])
    if len(s["votes"]) >= alive_count:
        if s.get("timer"): s["timer"].cancel()

async def mafia_resolve_voting(chat_id: int):
    """Подсчитывает результаты голосования и исключает игрока."""
    s = mafia_sessions.get(chat_id)
    if not s or s["phase"] != "voting": return
    s["phase"] = "ended_vote"
    
    if not s["votes"]:
        await bot.send_message(chat_id, "😂 Результат: Данные отсутствуют. Никто не исключен 👍.")
    else:
        from collections import Counter
        counts = Counter(s["votes"].values())
        max_v = max(counts.values())
        candidates = [tid for tid, v in counts.items() if v == max_v]
        target_id = random.choice(candidates)
        p = s["players"][target_id]
        p["is_alive"] = False
        await bot.send_message(chat_id, f"😂 Большинством голосов исключен объект <b>{p['name']}</b>.\nЕго истинная роль: <b>{p['role']}</b> 🍷.", parse_mode="HTML")
        
    if not await mafia_check_win(chat_id):
        await mafia_night_cycle(chat_id)

@dp.message(Command("мафия_старт", prefix="!/"))
async def mafia_init_cmd(m: Message):
    if not await is_admin(m): return
    if chat_id := m.chat.id:
        if chat_id in mafia_sessions or chat_id in bunker_sessions or chat_id in monopoly_sessions:
            return await m.answer("😂 Ошибка. Система уже занята другим процессом 👍.")
            
        mafia_sessions[chat_id] = {"players": {}, "active": False, "phase": "lobby"}
        kb = InlineKeyboardBuilder().button(text="Войти 🍷", callback_data="mf_join").button(text="Старт 📺", callback_data="mf_launch")
        await m.answer("😂 <b>Протокол 'МАФИЯ' активирован.</b>\nОжидание подключения объектов... 🍷", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "mf_join")
async def mafia_join_handler(c: CallbackQuery):
    s = mafia_sessions.get(c.message.chat.id)
    if not s or s["active"]: return
    profile = get_user_profile(c.from_user)
    s["players"][c.from_user.id] = {"name": profile["name"], "is_alive": True, "role": "Мирный"}
    await c.answer("Вы успешно авторизованы! 👍")

@dp.callback_query(F.data == "mf_launch")
async def mafia_launch_handler(c: CallbackQuery):
    cid = c.message.chat.id
    s = mafia_sessions.get(cid)
    if not s or len(s["players"]) < 4: 
        return await c.answer("Недостаточно объектов для старта (мин. 4)!", show_alert=True)
    
    s["active"] = True
    uids = list(s["players"].keys())
    random.shuffle(uids)
    
    # Распределение ролей
    s["players"][uids[0]]["role"] = "Мафия"
    s["players"][uids[1]]["role"] = "Доктор"
    s["players"][uids[2]]["role"] = "Комиссар"
    
    for uid, p in s["players"].items():
        try:
            await bot.send_message(uid, f"😂 Симуляция запущена.\nВаша персональная роль: <b>{p['role']}</b> 🍷.", parse_mode="HTML")
        except Exception: 
            pass
        
    await c.message.edit_text("😂 Роли распределены. Переход в ночной режим... 📺")
    await mafia_night_cycle(cid)

# --- МОДУЛЬ: МОНОПОЛИЯ (ЭКОНОМИЧЕСКИЙ ЦИКЛ) ---

def render_mono_keyboard(s: dict, actions: Optional[List[dict]] = None) -> types.InlineKeyboardMarkup:
    """Визуализирует игровое поле 4x4 с помощью кнопок."""
    builder = InlineKeyboardBuilder()
    
    def get_cell_text(idx):
        cell = s["board"][idx]
        occ = [p["emoji"] for p in s["players"].values() if p["pos"] == idx and not p["is_bankrupt"]]
        text = cell["short"]
        if cell.get("owner"):
            owner_data = s["players"][cell["owner"]]
            text += f"({owner_data['emoji']})"
        if occ:
            text += " " + "".join(occ)
        return text

    # Ряд 1
    builder.row(*[InlineKeyboardButton(text=get_cell_text(i), callback_data=f"mono_info_{i}") for i in range(0, 4)])
    # Ряд 2
    builder.row(InlineKeyboardButton(text=get_cell_text(15), callback_data="mono_info_15"), 
                InlineKeyboardButton(text="🏢", callback_data="none"), 
                InlineKeyboardButton(text="🏦", callback_data="none"), 
                InlineKeyboardButton(text=get_cell_text(4), callback_data="mono_info_4"))
    # Ряд 3
    builder.row(InlineKeyboardButton(text=get_cell_text(14), callback_data="mono_info_14"), 
                InlineKeyboardButton(text="💰", callback_data="none"), 
                InlineKeyboardButton(text="⚖️", callback_data="none"), 
                InlineKeyboardButton(text=get_cell_text(5), callback_data="mono_info_5"))
    # Ряд 4
    builder.row(*[InlineKeyboardButton(text=get_cell_text(i), callback_data=f"mono_info_{i}") for i in range(13, 9, -1)])
    
    if actions:
        for btn in actions:
            builder.row(InlineKeyboardButton(text=btn["text"], callback_data=btn["callback"]))
            
    return builder.as_markup()

async def mono_process_turn(chat_id: int):
    """Управляет очередностью ходов и победой."""
    s = monopoly_sessions.get(chat_id)
    if not s: return
    
    active_uids = [uid for uid, p in s["players"].items() if not p["is_bankrupt"]]
    if len(active_uids) == 1:
        winner = s["players"][active_uids[0]]
        await bot.send_message(chat_id, f"🏆 <b>Рыночный цикл завершен!</b>\nЕдинственный выживший объект: {winner['emoji']} <b>{winner['name']}</b>! 👍.", parse_mode="HTML")
        del monopoly_sessions[chat_id]
        return

    uid = s["turn_queue"][s["current_turn_idx"]]
    p = s["players"][uid]
    
    if p["is_bankrupt"]:
        s["current_turn_idx"] = (s["current_turn_idx"] + 1) % len(s["turn_queue"])
        return await mono_process_turn(chat_id)
        
    kb = render_mono_keyboard(s, [{"text": "Генерация шага (1-4) 🎲", "callback": f"mono_roll_{uid}"}])
    msg_txt = f"😂 Ход объекта: {p['emoji']} <b>{p['name']}</b>\n💰 Текущий капитал: <b>{p['balance']}$</b>"
    
    if s.get("board_msg_id"):
        try: 
            await bot.edit_message_text(msg_txt, chat_id, s["board_msg_id"], reply_markup=kb, parse_mode="HTML")
        except Exception: 
            s["board_msg_id"] = (await bot.send_message(chat_id, msg_txt, reply_markup=kb, parse_mode="HTML")).message_id
    else:
        msg = await bot.send_message(chat_id, msg_txt, reply_markup=kb, parse_mode="HTML")
        s["board_msg_id"] = msg.message_id

@dp.callback_query(F.data.startswith("mono_roll_"))
async def mono_roll_handler(c: CallbackQuery):
    """Логика броска кубика и перемещения по полю."""
    uid = int(c.data.split("_")[2])
    if c.from_user.id != uid: 
        return await c.answer("Сбой! Ожидайте своей очереди в системе 📺", show_alert=True)
    
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s: return
    
    p = s["players"][uid]
    roll = random.randint(1, 4)
    old_pos = p["pos"]
    p["pos"] = (p["pos"] + roll) % len(s["board"])
    
    # Бонус за круг
    if p["pos"] < old_pos:
        p["balance"] += 200
        await bot.send_message(cid, f"😂 {p['emoji']} <b>{p['name']}</b> пересек точку СТАРТ. Капитал увеличен (+200$) 🍷.", parse_mode="HTML")
        
    cell = s["board"][p["pos"]]
    res_text = f"😂 {p['emoji']} <b>{p['name']}</b> выбросил {roll}.\n📍 Текущая позиция: <b>{cell['name']}</b>."
    
    # Обработка типов ячеек
    if cell["type"] == "tax":
        tax_amt = cell.get("tax_amount", 100)
        p["balance"] -= tax_amt
        res_text += f"\n💸 Списание налога в пользу алгоритма: <b>-{tax_amt}$</b>."
        await bot.send_message(cid, res_text, parse_mode="HTML")
        await mono_check_bankruptcy(cid, s, uid)
    elif cell["type"] == "chance":
        mod = random.choice([100, 200, -50, -150])
        p["balance"] += mod
        res_text += f"\n✨ Аномалия 'Шанс': <b>{'+' if mod>0 else ''}{mod}$</b>."
        await bot.send_message(cid, res_text, parse_mode="HTML")
        await mono_check_bankruptcy(cid, s, uid)
    elif cell["type"] == "prop":
        if cell["owner"] is None:
            if p["balance"] >= cell["price"]:
                await bot.send_message(cid, res_text, parse_mode="HTML")
                # Предлагаем покупку
                kb = render_mono_keyboard(s, [
                    {"text": f"Приобрести за {cell['price']}$ 🍷", "callback": f"mono_buy_{uid}_{p['pos']}"},
                    {"text": "Пропустить сделку 👍", "callback": f"mono_pass_{uid}"}
                ])
                await bot.edit_message_reply_markup(cid, s["board_msg_id"], reply_markup=kb)
                return
            else:
                res_text += f"\n📺 Недостаточно ресурсов для покупки ({cell['price']}$)."
                await bot.send_message(cid, res_text, parse_mode="HTML")
                await mono_check_bankruptcy(cid, s, uid)
        elif cell["owner"] == uid:
            res_text += "\n🏠 Вы находитесь в своей собственности. Режим ожидания... 🍷"
            await bot.send_message(cid, res_text, parse_mode="HTML")
            await mono_check_bankruptcy(cid, s, uid)
        else:
            owner = s["players"][cell["owner"]]
            rent = cell["rent"]
            p["balance"] -= rent
            owner["balance"] += rent
            res_text += f"\n💳 Автоматическое списание ренты объекту {owner['emoji']} {owner['name']}: <b>{rent}$</b>."
            await bot.send_message(cid, res_text, parse_mode="HTML")
            await mono_check_bankruptcy(cid, s, uid)
    else:
        await bot.send_message(cid, res_text, parse_mode="HTML")
        await mono_check_bankruptcy(cid, s, uid)

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
    
    await bot.send_message(cid, f"😂 Сделка подтверждена. <b>{p['name']}</b> приобрел локацию '{cell['name']}' 🍷.", parse_mode="HTML")
    await mono_check_bankruptcy(cid, s, uid)

@dp.callback_query(F.data.startswith("mono_pass_"))
async def mono_pass_callback(c: CallbackQuery):
    uid = int(c.data.split("_")[2])
    cid, s = c.message.chat.id, monopoly_sessions.get(c.message.chat.id)
    if not s or c.from_user.id != uid: return
    
    await bot.send_message(cid, f"😂 Объект <b>{s['players'][uid]['name']}</b> отклонил финансовое предложение 📺.", parse_mode="HTML")
    await mono_check_bankruptcy(cid, s, uid)

async def mono_check_bankruptcy(cid: int, s: dict, uid: int):
    """Проверяет баланс и исключает игрока при нуле."""
    p = s["players"][uid]
    if p["balance"] < 0:
        p["is_bankrupt"] = True
        # Освобождение недвижимости
        for cell in s["board"]:
            if cell.get("owner") == uid: cell["owner"] = None
        await bot.send_message(cid, f"💀 Объект {p['emoji']} <b>{p['name']}</b> объявляет себя банкротом! Все активы изъяты 📺.", parse_mode="HTML")
    
    # Смена хода
    s["current_turn_idx"] = (s["current_turn_idx"] + 1) % len(s["turn_queue"])
    await asyncio.sleep(1.5)
    await mono_process_turn(cid)

@dp.message(Command("монополия_старт", prefix="!/"))
async def mono_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): 
        return await m.answer("😂 Система занята 👍.")
    
    monopoly_sessions[m.chat.id] = {"players": {}, "active": False, "board_msg_id": None}
    kb = InlineKeyboardBuilder().button(text="Авторизация 🍷", callback_data="mono_join").button(text="Запуск 😂", callback_data="mono_launch")
    await m.answer("😂 <b>Экономическая симуляция 'Монополия'.</b>\nОжидание инвесторов 🍷.", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- МОДУЛЬ: БУНКЕР (ПОЛНАЯ СИМУЛЯЦИЯ) ---

@dp.message(Command("бункер_старт", prefix="!/"))
async def bunker_init_cmd(m: Message):
    if not await is_admin(m): return
    if check_game_active(m.chat.id): return await m.answer("😂 Система уже в процессе 👍.")
    
    bunker_sessions[m.chat.id] = {"players": {}, "active": False, "round": 1}
    kb = InlineKeyboardBuilder().button(text="Присоединиться 🍷", callback_data="bn_join").button(text="Старт 😂", callback_data="bn_launch")
    await m.answer("😂 <b>Протокол 'БУНКЕР' в процессе загрузки.</b>\nОжидание авторизации объектов 🍷.", reply_markup=kb.as_markup(), parse_mode="HTML")

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
    if not s or len(s["players"]) < 3: 
        return await c.answer("Требуется минимум 3 объекта для стабильной симуляции! 👍", show_alert=True)
    
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
            msg = (f"😂 <b>Ваш персональный профиль Бункера:</b>\n\n"
                   f"🧬 Биология: {t['bio']}\n"
                   f"🛠 Профессия: {t['prof']}\n"
                   f"🩺 Здоровье: {t['health']}\n"
                   f"🎭 Черта: {t['trait']}\n"
                   f"🎸 Хобби: {t['hobby']}\n"
                   f"🎒 Багаж: {t['baggage']}\n\n"
                   f"Для вскрытия используйте кнопки в чате 📺.")
            await bot.send_message(uid, msg, parse_mode="HTML")
        except Exception: 
            pass
        
    await c.message.edit_text("😂 Симуляция запущена. Личные характеристики отправлены в ЛС 🍷.")
    await bunker_start_round(cid)

async def bunker_start_round(chat_id: int):
    """Запускает фазу обсуждения и выбора параметра."""
    s = bunker_sessions.get(chat_id)
    if not s: return
    s["phase"] = "discussion"
    for p in s["players"].values(): p["rev_this_round"] = False
    
    kb = InlineKeyboardBuilder()
    mapping = {"bio": "🧬 Биология", "prof": "🛠 Профессия", "health": "🩺 Здоровье", "trait": "🎭 Черта", "hobby": "🎸 Хобби", "baggage": "🎒 Багаж"}
    for k, v in mapping.items():
        kb.button(text=v, callback_data=f"bn_reveal_{k}")
    kb.button(text="Перейти к исключению 📺", callback_data=f"bn_vnow_{chat_id}")
    
    await bot.send_message(chat_id, f"😂 <b>Цикл {s['round']}.</b>\nФаза обсуждения (5 мин). Вскройте один из своих параметров 🍷.", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
    
    s["timer"] = asyncio.create_task(asyncio.sleep(300))
    try: await s["timer"]
    except asyncio.CancelledError: pass
    await bunker_start_voting(chat_id)

@dp.callback_query(F.data.startswith("bn_reveal_"))
async def bunker_reveal_handler(c: CallbackQuery):
    trait = c.data.split("_")[2]
    cid, uid = c.message.chat.id, c.from_user.id
    s = bunker_sessions.get(cid)
    if not s or s["phase"] != "discussion": return await c.answer("Вскрытие в данной фазе запрещено!")
    
    p = s["players"].get(uid)
    if not p: return await c.answer("Вы не числитесь в списке объектов игры!", show_alert=True)
    if p["rev_this_round"]: return await c.answer("Лимит вскрытий исчерпан в этом цикле! 😂", show_alert=True)
    if p["revealed"][trait]: return await c.answer("Этот параметр уже находится в общем доступе.")
    
    p["revealed"][trait] = True
    p["rev_this_round"] = True
    val = p["traits"][trait]
    
    trait_ru = {"bio": "Биологию", "prof": "Профессию", "health": "Здоровье", "trait": "Черту", "hobby": "Хобби", "baggage": "Багаж"}[trait]
    await bot.send_message(cid, f"😂 Объект <b>{p['name']}</b> открыл доступ к параметру <b>{trait_ru}</b>: {val} 👍.", parse_mode="HTML")
    await c.answer("Данные успешно синхронизированы! 🍷")

async def bunker_start_voting(chat_id: int):
    """Инициирует голосование на вылет."""
    s = bunker_sessions.get(chat_id)
    if not s or s["phase"] == "voting": return
    s["phase"], s["votes"] = "voting", {}
    
    kb = InlineKeyboardBuilder()
    for uid, p in s["players"].items():
        kb.button(text=p["name"], callback_data=f"bn_v_{chat_id}_{uid}")
        
    await bot.send_message(chat_id, "😂 <b>Таймер обсуждения истек!</b>\nВыберите лишний объект для исключения из Бункера 📺:", reply_markup=kb.adjust(2).as_markup(), parse_mode="HTML")
    
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
    if uid in s["votes"]: return await c.answer("Вы уже сделали выбор.")
    
    s["votes"][uid] = tid
    await c.answer(f"Голос против объекта {s['players'][tid]['name']} принят 🍷.")
    
    if len(s["votes"]) >= len(s["players"]):
        if s.get("timer"): s["timer"].cancel()

async def bunker_resolve_voting(chat_id: int):
    """Исключает игрока и проверяет завершение игры."""
    s = bunker_sessions.get(chat_id)
    if not s or s["phase"] != "voting": return
    
    if s["votes"]:
        from collections import Counter
        counts = Counter(s["votes"].values())
        target_id = random.choice([tid for tid, v in counts.items() if v == max(counts.values())])
        name = s["players"].pop(target_id)["name"]
        await bot.send_message(chat_id, f"😂 По решению большинства объект <b>{name}</b> покидает Бункер. Люк закрыт 👍.", parse_mode="HTML")
    else:
        await bot.send_message(chat_id, "😂 Система не получила голосов. Все объекты остаются на борту 📺.")
        
    if len(s["players"]) <= 3:
        res_str = "😂 <b>Симуляция успешно завершена!</b>\nВыжившие в Бункере:\n\n"
        for p in s["players"].values():
            res_str += f"👤 <b>{p['name']}</b>: {p['traits']['prof']}, {p['traits']['health']}\n"
        await bot.send_message(chat_id, res_str + "\n🍷 Алгоритм переведен в режим ожидания.", parse_mode="HTML")
        del bunker_sessions[chat_id]
    else:
        s["round"] += 1
        await bunker_start_round(chat_id)

# --- МОДУЛЬ: КРЕСТИКИ-НОЛИКИ (ЛОГИЧЕСКИЙ ДУЭЛЬ) ---

@dp.message(Command("крестики_нолики", prefix="!/"))
async def ttt_init_cmd(m: Message):
    if check_game_active(m.chat.id): return await m.answer("😂 Занято 👍.")
    tictactoe_games[m.chat.id] = {"board": [None]*9, "turn": "X", "players": []}
    kb = InlineKeyboardBuilder()
    for i in range(9): kb.button(text=" ", callback_data=f"ttt_{i}")
    await m.answer("😂 <b>Крестики-нолики.</b>\nЖду двух участников симуляции 🍷.", reply_markup=kb.adjust(3).as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("ttt_"))
async def ttt_handler(c: CallbackQuery):
    idx = int(c.data.split("_")[1])
    cid, uid = c.message.chat.id, c.from_user.id
    g = tictactoe_games.get(cid)
    if not g: return
    
    # Авторизация игроков
    p_ids = [p["id"] for p in g["players"]]
    if uid not in p_ids:
        if len(g["players"]) < 2:
            g["players"].append({"id": uid, "name": c.from_user.first_name})
        else: return await c.answer("Слоты участников заполнены!")
        
    # Ход
    curr_idx = 0 if g["turn"] == "X" else 1
    if g["players"][curr_idx]["id"] != uid: return await c.answer("Ожидание хода другого объекта 📺!", show_alert=True)
    if g["board"][idx]: return await c.answer("Клетка уже активирована!")
    
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
        await c.message.edit_text(f"😂 Победил объект <b>{g['players'][curr_idx]['name']}</b>! 🍷", reply_markup=kb.as_markup(), parse_mode="HTML")
        del tictactoe_games[cid]
    elif None not in g["board"]:
        await c.message.edit_text(f"😂 <b>Ничья!</b> Система перегружена 👍.", reply_markup=kb.as_markup(), parse_mode="HTML")
        del tictactoe_games[cid]
    else:
        g["turn"] = "O" if g["turn"] == "X" else "X"
        await c.message.edit_text(f"😂 {p1} (X) против {p2} (O).\nХод объекта: <b>{g['turn']}</b> 📺.", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- СЕРВИСНЫЕ И АДМИНИСТРАТИВНЫЕ КОМАНДЫ ---

@dp.message(Command("старт", "start", "help", prefix="!/"))
async def start_help_cmd(m: Message):
    """Выводит справочную информацию по системе."""
    text = (
        "🍷 <b>СИСТЕМА ДИОНИС v3.5 (SERVER EDITION)</b>\n"
        "Приветствую. Я — развлекательный алгоритм с глубоким обучением.\n\n"
        "👤 <b>Профиль:</b>\n"
        "<code>!ник [Имя]</code> — смена идентификатора\n"
        "<code>!стикер [Emoji]</code> — визуальный маркер\n\n"
        "🎮 <b>Групповые симуляции:</b>\n"
        "<code>!мафия_старт</code> — запуск 'Мафии'\n"
        "<code>!бункер_старт</code> — запуск протокола 'Бункер'\n"
        "<code>!монополия_старт</code> — экономический цикл\n"
        "<code>!крестики_нолики</code> — логический дуэль\n\n"
        "🧩 <b>Мини-модули:</b>\n"
        "<code>!правда</code>, <code>!действие</code>, <code>!яникогдане</code>\n"
        "<code>!назови5</code>, <code>!назови7</code>, <code>!крокодил</code>\n"
        "<code>!тест</code>, <code>!бинго</code>, <code>!кмк</code>\n\n"
        "🛠 <b>База данных (Админ):</b>\n"
        "<code>!добавить_[категория] [текст]</code>"
    )
    await m.answer(text, parse_mode="HTML")

@dp.message(Command("правда", "действие", "яникогдане", "назови5", "назови7", prefix="!/"))
async def games_handler(m: Message, command: CommandObject):
    """Универсальный обработчик мини-игр."""
    cmd = command.command
    db_map = {"правда": TRUTH_DB, "действие": DARE_DB, "яникогдане": NHIE_DB, "назови5": N5_DB, "назови7": N7_DB}
    if cmd in db_map:
        await m.answer(f"😂 <b>{cmd.upper()}:</b> {random.choice(db_map[cmd])} 🍷", parse_mode="HTML")

@dp.message(Command("тест", "бинго", "кмк", prefix="!/"))
async def modules_handler(m: Message, command: CommandObject):
    """Обработка специальных модулей контента."""
    cmd = command.command
    if cmd == "тест":
        await m.answer(f"😂 Сгенерирован психологический опрос:\n{random.choice(TESTS_DB)} 🍷")
    elif cmd == "бинго":
        await m.answer_photo(photo=random.choice(BINGO_DB), caption="😂 Матрица вероятностей 'Бинго' сформирована 👍.")
    elif cmd == "кмк":
        s = random.sample(KMK_CHARACTERS, 3)
        await m.answer(f"😂 Матрица выбора KMK:\n1. <b>{s[0]}</b>\n2. <b>{s[1]}</b>\n3. <b>{s[2]}</b> 🍷", parse_mode="HTML")

@dp.message(Command("крокодил", prefix="!/"))
async def croc_cmd(m: Message):
    """Отправляет секретное слово в ЛС игроку."""
    word = random.choice(CROC_WORDS)
    try:
        await bot.send_message(m.from_user.id, f"😂 Секретное слово для передачи: <b>{word}</b> 📺.")
        await m.answer(f"😂 Параметры переданы объекту <b>{m.from_user.first_name}</b> 🍷.")
    except Exception:
        await m.answer("😂 Ошибка! Напишите мне в ЛС, чтобы я мог отправлять данные 👍.")

@dp.message(Command(F.string.startswith("добавить_"), prefix="!/"))
async def add_item_handler(m: Message, command: CommandObject):
    """Позволяет админам расширять базы данных на лету."""
    if not await is_admin(m): return
    category = command.command.replace("добавить_", "")
    if not command.args: return await m.answer("😂 Пустой ввод. Укажите текст после команды.")
    
    add_custom_item_db(category, command.args, m.from_user.id)
    # Мгновенно обновляем в оперативной памяти
    mem_map = {"test": TESTS_DB, "bingo": BINGO_DB, "croc": CROC_WORDS, "kmk": KMK_CHARACTERS, "truth": TRUTH_DB, "dare": DARE_DB, "nhie": NHIE_DB, "n5": N5_DB, "n7": N7_DB}
    if category in mem_map:
        mem_map[category].append(command.args)
        await m.answer(f"😂 База данных '{category}' расширена. Алгоритм обучается 📺.")

# --- ФОНОВЫЕ ЗАДАЧИ СЕРВЕРА ---

async def health_check_loop():
    """
    Каждый час отправляет уведомление супер-админу о стабильной работе системы.
    """
    while True:
        try:
            await bot.send_message(SUPER_ADMIN_ID, "🍷 Алгоритм ДИОНИС: Статус 'Работаю'. Все системы стабильны.")
            logger.info("SYSTEM: Уведомление 'Работаю' отправлено супер-админу.")
        except Exception as e:
            logger.error(f"SYSTEM ERROR: Ошибка отправки статуса: {e}")
        
        await asyncio.sleep(3600)  # Интервал: 1 час

# --- ЗАПУСК И ЗАВЕРШЕНИЕ ---

async def on_shutdown_logic():
    """Корректное закрытие всех сессий перед выключением."""
    logger.info("SHUTDOWN: Инициировано завершение работы бота.")
    await bot.session.close()

async def main():
    # Стартовая инициализация
    init_db()
    
    # Регистрация систем защиты
    dp.message.middleware(RestrictChatMiddleware())
    dp.callback_query.middleware(RestrictChatMiddleware())
    
    # Запуск фонового процесса мониторинга
    asyncio.create_task(health_check_loop())
    
    logger.info("SYSTEM START: Бот Дионис запущен на сервере.")
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"CRITICAL RUNTIME ERROR: {e}", exc_info=True)
    finally:
        await on_shutdown_logic()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("SYSTEM: Бот остановлен вручную.")