import os
import sys
import json
import logging
import asyncio
import sqlite3
from datetime import datetime, time
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse

import pytz
from dateparser import parse
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

# Настройка event loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
BOT_TOKEN = os.getenv("BOT_TOKEN")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

# Инициализация клиентов
storage = MemoryStorage()
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher(storage=storage, close_old_connections=True, ignore_old_updates=True)
client = TelegramClient('session_name', API_ID, API_HASH)

# Состояния бота
class Form(StatesGroup):
    add_group = State()
    remove_group = State()
    set_text = State()
    set_time = State()
    add_template = State()
    remove_template = State()
    add_tags = State()
    filter_by_tag = State()
    confirm_send = State()
    add_schedule = State()
    remove_schedule = State()

class Database:
    def __init__(self):
        self.conn = sqlite3.connect('bot_data.db', check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY,
                link TEXT UNIQUE,
                tags TEXT DEFAULT ''
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                content TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY,
                text TEXT,
                send_time TEXT,
                groups TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_groups_tags ON groups(tags)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_posts_time ON scheduled_posts(send_time)')
        self.conn.commit()
    
    # Методы работы с группами
    def add_group(self, link: str, tags: str = ""):
        cursor = self.conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO groups (link, tags) VALUES (?, ?)', (link, tags))
        self.conn.commit()
    
    def remove_group(self, group_id: int):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM groups WHERE id = ?', (group_id,))
        self.conn.commit()
    
    def get_groups(self, tag: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor()
        if tag:
            cursor.execute('SELECT * FROM groups WHERE tags LIKE ?', (f"%{tag}%",))
        else:
            cursor.execute('SELECT * FROM groups')
        return [{'id': row[0], 'link': row[1], 'tags': row[2]} for row in cursor.fetchall()]
    
    def update_group_tags(self, group_id: int, tags: str):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE groups SET tags = ? WHERE id = ?', (tags, group_id))
        self.conn.commit()
    
    # Методы работы с шаблонами
    def add_template(self, name: str, content: str):
        cursor = self.conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO templates (name, content) VALUES (?, ?)', (name, content))
        self.conn.commit()
    
    def remove_template(self, template_id: int):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM templates WHERE id = ?', (template_id,))
        self.conn.commit()
    
    def get_templates(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM templates')
        return [{'id': row[0], 'name': row[1], 'content': row[2]} for row in cursor.fetchall()]
    
    # Методы работы с настройками
    def get_setting(self, key: str) -> Optional[str]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def set_setting(self, key: str, value: str):
        cursor = self.conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
        self.conn.commit()
    
    # Методы работы с расписанием

    def add_scheduled_post(self, text: str, send_time: str, groups: List[str]):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO scheduled_posts (text, send_time, groups) VALUES (?, ?, ?)',
        (text, send_time, json.dumps(groups))  # Добавлена закрывающая скобка
    )
        self.conn.commit()
    
    def remove_scheduled_post(self, post_id: int):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM scheduled_posts WHERE id = ?', (post_id,))
        self.conn.commit()
    
    def get_scheduled_posts(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM scheduled_posts WHERE is_active = 1')
        return [
            {
                'id': row[0],
                'text': row[1],
                'send_time': row[2],
                'groups': json.loads(row[3]),
                'is_active': bool(row[4])
            }
            for row in cursor.fetchall()
        ]
    
    def deactivate_scheduled_post(self, post_id: int):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE scheduled_posts SET is_active = 0 WHERE id = ?', (post_id,))
        self.conn.commit()

db = Database()

class Stats:
    def __init__(self):
        self.sent_count = 0
        self.error_count = 0
    
    def increment_sent(self):
        self.sent_count += 1
    
    def increment_errors(self):
        self.error_count += 1

stats = Stats()

# ======================
# ИНЛАЙН КЛАВИАТУРЫ
# ======================

def get_main_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📌 Группы", callback_data="groups_menu"),
        InlineKeyboardButton(text="📝 Контент", callback_data="content_menu"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="⏰ Расписание", callback_data="scheduler_menu"),
        InlineKeyboardButton(text="📊 Статистика", callback_data="show_stats"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings_menu"),
        InlineKeyboardButton(text="🆘 Помощь", callback_data="show_help"),
        width=2
    )
    return builder.as_markup()

def get_groups_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Добавить", callback_data="add_group"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data="remove_group"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="🏷 Теги", callback_data="group_tags"),
        InlineKeyboardButton(text="🔍 Фильтр", callback_data="filter_by_tag"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список", callback_data="view_groups"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu"),
        width=2
    )
    return builder.as_markup()

def get_content_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📝 Текст", callback_data="set_text"),
        InlineKeyboardButton(text="📋 Шаблоны", callback_data="templates_menu"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="👁 Предпросмотр", callback_data="preview"),
        InlineKeyboardButton(text="🚀 Отправить", callback_data="confirm_send"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu"),
        width=1
    )
    return builder.as_markup()

def get_templates_menu_kb() -> InlineKeyboardMarkup:
    templates = db.get_templates()
    builder = InlineKeyboardBuilder()
    
    for template in templates:
        builder.button(
            text=f"📝 {template['name'][:15]}", 
            callback_data=f"use_template_{template['id']}"
        )
    
    builder.adjust(2)
    builder.row(
        InlineKeyboardButton(text="➕ Добавить", callback_data="add_template"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data="remove_template"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="content_menu"),
        width=1
    )
    return builder.as_markup()

def get_scheduler_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🕒 Установить время", callback_data="set_time"),
        InlineKeyboardButton(text="📋 Список", callback_data="view_schedule"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="➕ Добавить", callback_data="add_schedule"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data="remove_schedule"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu"),
        width=1
    )
    return builder.as_markup()

def get_confirmation_kb(action: str = "") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_{action}"),
        InlineKeyboardButton(text="❌ Нет", callback_data=f"cancel_{action}"),
        width=2
    )
    if action == "send":
        builder.button(text="⏱ Отложить", callback_data="schedule_later")
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu"),
        width=1
    )
    return builder.as_markup()

# ======================
# ОСНОВНЫЕ КОМАНДЫ
# ======================

@dp.message(Command("start", "help", "menu"))
async def cmd_start(message: types.Message):
    try:
        await message.answer(
            "🤖 Бот для управления рассылками в Telegram\n\n"
            "Используйте кнопки ниже для навигации:",
            reply_markup=get_main_menu_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка в cmd_start: {e}")

@dp.callback_query(F.data == "main_menu")
async def main_menu(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "📋 Главное меню:",
            reply_markup=get_main_menu_kb()
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в main_menu: {e}")
    finally:
        await callback_query.answer()

# ======================
# ОБРАБОТЧИКИ ГРУПП
# ======================

@dp.callback_query(F.data == "groups_menu")
async def groups_menu(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "📌 Управление группами:",
            reply_markup=get_groups_menu_kb()
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в groups_menu: {e}")
    finally:
        await callback_query.answer()

@dp.callback_query(F.data == "add_group")
async def add_group_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.add_group)
    try:
        await callback_query.message.edit_text(
            "Введите ссылку на группу в формате:\n"
            "• https://t.me/username\n"
            "• @username\n\n"
            "✏️ Для отмены введите /cancel",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="groups_menu")]
            ])
        )
    except Exception as e:
        logger.error(f"Ошибка в add_group_start: {e}")
    finally:
        await callback_query.answer()

@dp.message(Form.add_group)
async def add_group_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Добавление группы отменено", reply_markup=get_groups_menu_kb())
        return
    
    link = message.text.strip()
    if link.startswith('@'):
        link = f"https://t.me/{link[1:]}"
    
    parsed = urlparse(link)
    if not all([parsed.scheme, parsed.netloc]) or not parsed.netloc.endswith('t.me'):
        await message.answer(
            "❌ Неверный формат ссылки. Используйте:\n"
            "• https://t.me/username\n"
            "• @username",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="groups_menu")]
            ])
        )
        return
    
    db.add_group(link)
    await message.answer(
        f"✅ Группа {link} добавлена!",
        reply_markup=get_groups_menu_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "remove_group")
async def remove_group_start(callback_query: types.CallbackQuery, state: FSMContext):
    groups = db.get_groups()
    if not groups:
        await callback_query.answer("Список групп пуст", show_alert=True)
        return
    
    groups_list = "\n".join(f"{i+1}. {g['link']}" for i, g in enumerate(groups))
    await callback_query.message.edit_text(
        f"Введите номер группы для удаления:\n\n{groups_list}\n\n"
        "✏️ Для отмены введите /cancel",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="groups_menu")]
        ]))
    await state.set_state(Form.remove_group)

@dp.message(Form.remove_group)
async def remove_group_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Удаление отменено", reply_markup=get_groups_menu_kb())
        return
    
    try:
        group_num = int(message.text.strip())
        groups = db.get_groups()
        
        if 1 <= group_num <= len(groups):
            db.remove_group(groups[group_num-1]['id'])
            await message.answer(f"✅ Группа {groups[group_num-1]['link']} удалена!", 
                               reply_markup=get_groups_menu_kb())
        else:
            await message.answer("❌ Неверный номер группы", 
                               reply_markup=get_groups_menu_kb())
    except ValueError:
        await message.answer("❌ Введите число - номер группы", 
                           reply_markup=get_groups_menu_kb())
    finally:
        await state.clear()

@dp.callback_query(F.data == "view_groups")
async def view_groups(callback_query: types.CallbackQuery):
    try:
        groups = db.get_groups()
        if not groups:
            await callback_query.answer("📭 Список групп пуст", show_alert=True)
            return
        
        groups_list = "\n".join(
            f"{i+1}. {g['link']} {'🏷 ' + g['tags'] if g['tags'] else ''}"
            for i, g in enumerate(groups))
        
        await callback_query.message.edit_text(
            f"📋 Список групп ({len(groups)}):\n\n{groups_list}",
            reply_markup=get_groups_menu_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка в view_groups: {e}")
        await callback_query.answer("⚠️ Произошла ошибка", show_alert=True)
    finally:
        await callback_query.answer()

@dp.callback_query(F.data == "group_tags")
async def group_tags_start(callback_query: types.CallbackQuery):
    groups = db.get_groups()
    builder = InlineKeyboardBuilder()
    
    for group in groups:
        builder.button(
            text=f"{group['link']} ({group['tags'] or 'нет тегов'})", 
            callback_data=f"edit_tags_{group['id']}"
        )
    
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="groups_menu"))
    
    await callback_query.message.edit_text(
        "Выберите группу для редактирования тегов:",
        reply_markup=builder.as_markup()
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("edit_tags_"))
async def edit_tags_start(callback_query: types.CallbackQuery, state: FSMContext):
    group_id = int(callback_query.data.split("_")[-1])
    await state.update_data(group_id=group_id)
    await state.set_state(Form.add_tags)
    
    await callback_query.message.edit_text(
        "Введите теги через запятую (например: новости,основное,важное):\n\n"
        "✏️ Для отмены введите /cancel",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="group_tags")]
        ]))
    await callback_query.answer()

@dp.message(Form.add_tags)
async def edit_tags_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Редактирование тегов отменено", 
                           reply_markup=get_groups_menu_kb())
        return
    
    data = await state.get_data()
    tags = message.text.strip()
    db.update_group_tags(data['group_id'], tags)
    
    await message.answer("✅ Теги обновлены!", reply_markup=get_groups_menu_kb())
    await state.clear()

@dp.callback_query(F.data == "filter_by_tag")
async def filter_by_tag_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.filter_by_tag)
    await callback_query.message.edit_text(
        "Введите тег для фильтрации (или оставьте пустым для всех групп):\n\n"
        "✏️ Для отмены введите /cancel",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="groups_menu")]
        ]))
    await callback_query.answer()

@dp.message(Form.filter_by_tag)
async def filter_by_tag_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Фильтрация отменена", reply_markup=get_groups_menu_kb())
        return
    
    tag = message.text.strip() if message.text.strip() else None
    groups = db.get_groups(tag)
    
    if not groups:
        await message.answer("❌ Группы с таким тегом не найдены", 
                           reply_markup=get_groups_menu_kb())
        return
    
    groups_list = "\n".join(f"{g['link']} - {g['tags']}" for g in groups)
    await message.answer(
        f"📋 Найдено групп: {len(groups)}\n\n{groups_list}",
        reply_markup=get_groups_menu_kb()
    )
    await state.clear()

# ======================
# ОБРАБОТЧИКИ КОНТЕНТА
# ======================

@dp.callback_query(F.data == "content_menu")
async def content_menu(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "📝 Управление контентом:",
            reply_markup=get_content_menu_kb()
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в content_menu: {e}")
    finally:
        await callback_query.answer()

@dp.callback_query(F.data == "set_text")
async def set_text_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.set_text)
    try:
        await callback_query.message.edit_text(
            "Введите текст для рассылки:\n\n"
            "✏️ Для отмены введите /cancel",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="content_menu")]
            ]))
    except Exception as e:
        logger.error(f"Ошибка в set_text_start: {e}")
    finally:
        await callback_query.answer()

@dp.message(Form.set_text)
async def set_text_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Установка текста отменена", reply_markup=get_content_menu_kb())
        return
    
    db.set_setting('current_text', message.text)
    await message.answer(
        "✅ Текст сохранен!",
        reply_markup=get_content_menu_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "templates_menu")
async def templates_menu(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "📋 Управление шаблонами:",
            reply_markup=get_templates_menu_kb()
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в templates_menu: {e}")
    finally:
        await callback_query.answer()

@dp.callback_query(F.data.startswith("use_template_"))
async def use_template(callback_query: types.CallbackQuery):
    template_id = int(callback_query.data.split("_")[-1])
    templates = db.get_templates()
    template = next((t for t in templates if t['id'] == template_id), None)
    
    if not template:
        await callback_query.answer("❌ Шаблон не найден", show_alert=True)
        return
    
    db.set_setting('current_text', template['content'])
    await callback_query.message.edit_text(
        f"✅ Шаблон '{template['name']}' применен!\n\n"
        f"Текст:\n{template['content']}",
        reply_markup=get_content_menu_kb()
    )
    await callback_query.answer()

@dp.callback_query(F.data == "add_template")
async def add_template_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.add_template)
    try:
        await callback_query.message.edit_text(
            "Введите название и содержимое шаблона в формате:\n\n"
            "Название шаблона\n"
            "/\n"
            "Содержимое шаблона\n\n"
            "✏️ Для отмены введите /cancel",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="templates_menu")]
            ]))
    except Exception as e:
        logger.error(f"Ошибка в add_template_start: {e}")
    finally:
        await callback_query.answer()

@dp.message(Form.add_template)
async def add_template_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Добавление шаблона отменено", reply_markup=get_templates_menu_kb())
        return
    
    try:
        name, content = message.text.split("/", 1)
        name = name.strip()
        content = content.strip()
        
        db.add_template(name, content)
        await message.answer(
            f"✅ Шаблон '{name}' добавлен!",
            reply_markup=get_templates_menu_kb()
        )
    except ValueError:
        await message.answer(
            "❌ Неверный формат. Используйте:\n\n"
            "Название шаблона\n"
            "/\n"
            "Содержимое шаблона",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="templates_menu")]
            ]))
    finally:
        await state.clear()

@dp.callback_query(F.data == "remove_template")
async def remove_template_start(callback_query: types.CallbackQuery):
    templates = db.get_templates()
    if not templates:
        await callback_query.answer("Список шаблонов пуст", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for template in templates:
        builder.button(
            text=f"🗑 {template['name']}", 
            callback_data=f"confirm_remove_template_{template['id']}"
        )
    
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="templates_menu"))
    
    await callback_query.message.edit_text(
        "Выберите шаблон для удаления:",
        reply_markup=builder.as_markup()
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("confirm_remove_template_"))
async def confirm_remove_template(callback_query: types.CallbackQuery):
    template_id = int(callback_query.data.split("_")[-1])
    templates = db.get_templates()
    template = next((t for t in templates if t['id'] == template_id), None)
    
    if not template:
        await callback_query.answer("❌ Шаблон не найден", show_alert=True)
        return
    
    await callback_query.message.edit_text(
        f"Вы уверены, что хотите удалить шаблон '{template['name']}'?",
        reply_markup=get_confirmation_kb(f"remove_template_{template_id}")
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("confirm_remove_template_"))
async def remove_template_process(callback_query: types.CallbackQuery):
    template_id = int(callback_query.data.split("_")[-1])
    db.remove_template(template_id)
    
    await callback_query.message.edit_text(
        "✅ Шаблон удален!",
        reply_markup=get_templates_menu_kb()
    )
    await callback_query.answer()

@dp.callback_query(F.data == "preview")
async def preview_content(callback_query: types.CallbackQuery):
    try:
        text = db.get_setting('current_text')
        if not text:
            await callback_query.answer("❌ Текст не установлен", show_alert=True)
            return
        
        await callback_query.message.edit_text(
            f"👁 Предпросмотр:\n\n{text}",
            reply_markup=get_content_menu_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка в preview_content: {e}")
        await callback_query.answer("⚠️ Произошла ошибка", show_alert=True)
    finally:
        await callback_query.answer()

# ======================
# ОБРАБОТЧИКИ ОТПРАВКИ
# ======================

async def send_to_group(group_link: str, text: str, max_retries: int = 3) -> bool:
    for attempt in range(max_retries):
        try:
            await client.send_message(
                group_link,
                text,
                parse_mode="HTML"
            )
            stats.increment_sent()
            await asyncio.sleep(1)
            return True
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"FloodWait: ждем {wait_time} сек")
            await asyncio.sleep(wait_time)
            continue
        except Exception as e:
            logger.error(f"Ошибка при отправке (попытка {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
    
    stats.increment_errors()
    return False

@dp.callback_query(F.data == "confirm_send")
async def confirm_send(callback_query: types.CallbackQuery):
    try:
        text = db.get_setting('current_text')
        groups = db.get_groups()
        
        if not text or not groups:
            await callback_query.answer("❌ Текст или группы не установлены", show_alert=True)
            return
        
        await callback_query.message.edit_text("⏳ Начинаю отправку...")
        
        success = 0
        errors = 0
        
        for group in groups:
            if await send_to_group(group['link'], text):
                success += 1
            else:
                errors += 1
        
        await callback_query.message.edit_text(
            f"✅ Отправка завершена!\n\n"
            f"• Успешно: {success}\n"
            f"• Ошибок: {errors}",
            reply_markup=get_main_menu_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка в confirm_send: {e}")
        await callback_query.answer("❌ Ошибка при отправке", show_alert=True)
    finally:
        await callback_query.answer()

# ======================
# ОБРАБОТЧИКИ РАСПИСАНИЯ
# ======================

@dp.callback_query(F.data == "scheduler_menu")
async def scheduler_menu(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "⏰ Управление расписанием:",
            reply_markup=get_scheduler_menu_kb()
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в scheduler_menu: {e}")
    finally:
        await callback_query.answer()

@dp.callback_query(F.data == "set_time")
async def set_time_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.set_time)
    try:
        await callback_query.message.edit_text(
            "Введите время отправки в формате ЧЧ:ММ (например, 15:30):\n\n"
            "✏️ Для отмены введите /cancel",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="scheduler_menu")]
            ]))
    except Exception as e:
        logger.error(f"Ошибка в set_time_start: {e}")
    finally:
        await callback_query.answer()

@dp.message(Form.set_time)
async def set_time_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Установка времени отменена", reply_markup=get_scheduler_menu_kb())
        return
    
    try:
        time_obj = datetime.strptime(message.text, "%H:%M").time()
        db.set_setting('scheduled_time', message.text)
        await message.answer(
            f"✅ Время отправки установлено на {message.text}",
            reply_markup=get_scheduler_menu_kb()
        )
    except ValueError:
        await message.answer(
            "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 15:30)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="scheduler_menu")]
            ]))
    finally:
        await state.clear()

@dp.callback_query(F.data == "add_schedule")
async def add_schedule_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.add_schedule)
    try:
        await callback_query.message.edit_text(
            "Введите время и текст для запланированной отправки в формате:\n\n"
            "Время (ЧЧ:ММ)\n"
            "/\n"
            "Текст сообщения\n\n"
            "✏️ Для отмены введите /cancel",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="scheduler_menu")]
            ]))
    except Exception as e:
        logger.error(f"Ошибка в add_schedule_start: {e}")
    finally:
        await callback_query.answer()

@dp.message(Form.add_schedule)
async def add_schedule_process(message: types.Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Добавление расписания отменено", reply_markup=get_scheduler_menu_kb())
        return
    
    try:
        time_part, text = message.text.split("/", 1)
        time_str = time_part.strip()
        text = text.strip()
        
        # Проверка формата времени
        datetime.strptime(time_str, "%H:%M").time()
        
        groups = [g['link'] for g in db.get_groups()]
        if not groups:
            await message.answer("❌ Нет групп для отправки", reply_markup=get_scheduler_menu_kb())
            return
        
        db.add_scheduled_post(text, time_str, groups)
        await message.answer(
            f"✅ Запланированная отправка добавлена на {time_str}",
            reply_markup=get_scheduler_menu_kb()
        )
    except ValueError as e:
        await message.answer(
            "❌ Неверный формат. Используйте:\n\n"
            "Время (ЧЧ:ММ)\n"
            "/\n"
            "Текст сообщения",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="scheduler_menu")]
            ]))
    finally:
        await state.clear()

@dp.callback_query(F.data == "view_schedule")
async def view_schedule(callback_query: types.CallbackQuery):
    posts = db.get_scheduled_posts()
    if not posts:
        await callback_query.answer("Список запланированных постов пуст", show_alert=True)
        return
    
    posts_list = []
    for post in posts:
        groups = ", ".join(post['groups']) if isinstance(post['groups'], list) else post['groups']
        posts_list.append(
            f"⏰ {post['send_time']}\n"
            f"📝 {post['text'][:50]}...\n"
            f"👥 Группы: {groups}\n"
            f"ID: {post['id']}\n"
        )
    
    await callback_query.message.edit_text(
        "📅 Запланированные посты:\n\n" + "\n".join(posts_list),
        reply_markup=get_scheduler_menu_kb()
    )
    await callback_query.answer()

@dp.callback_query(F.data == "remove_schedule")
async def remove_schedule_start(callback_query: types.CallbackQuery, state: FSMContext):
    posts = db.get_scheduled_posts()
    if not posts:
        await callback_query.answer("Список запланированных постов пуст", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for post in posts:
        builder.button(
            text=f"🗑 {post['send_time']} - {post['text'][:20]}...", 
            callback_data=f"confirm_remove_schedule_{post['id']}"
        )
    
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="scheduler_menu"))
    
    await callback_query.message.edit_text(
        "Выберите запланированную отправку для удаления:",
        reply_markup=builder.as_markup()
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("confirm_remove_schedule_"))
async def confirm_remove_schedule(callback_query: types.CallbackQuery):
    post_id = int(callback_query.data.split("_")[-1])
    posts = db.get_scheduled_posts()
    post = next((p for p in posts if p['id'] == post_id), None)
    
    if not post:
        await callback_query.answer("❌ Запланированная отправка не найдена", show_alert=True)
        return
    
    await callback_query.message.edit_text(
        f"Вы уверены, что хотите удалить запланированную отправку на {post['send_time']}?",
        reply_markup=get_confirmation_kb(f"remove_schedule_{post_id}")
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("confirm_remove_schedule_"))
async def remove_schedule_process(callback_query: types.CallbackQuery):
    post_id = int(callback_query.data.split("_")[-1])
    db.remove_scheduled_post(post_id)
    
    await callback_query.message.edit_text(
        "✅ Запланированная отправка удалена!",
        reply_markup=get_scheduler_menu_kb()
    )
    await callback_query.answer()

# ======================
# ОБРАБОТЧИКИ СТАТИСТИКИ И ПОМОЩИ
# ======================

@dp.callback_query(F.data == "show_stats")
async def show_stats(callback_query: types.CallbackQuery):
    try:
        groups_count = len(db.get_groups())
        templates_count = len(db.get_templates())
        posts_count = len(db.get_scheduled_posts())
        
        await callback_query.message.edit_text(
            f"📊 Статистика:\n\n"
            f"• Групп: {groups_count}\n"
            f"• Шаблонов: {templates_count}\n"
            f"• Запланированных постов: {posts_count}\n"
            f"• Отправлено сообщений: {stats.sent_count}\n"
            f"• Ошибок отправки: {stats.error_count}",
            reply_markup=get_main_menu_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка в show_stats: {e}")
        await callback_query.answer("⚠️ Произошла ошибка", show_alert=True)
    finally:
        await callback_query.answer()

@dp.callback_query(F.data == "show_help")
async def show_help(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "🆘 Помощь:\n\n"
            "1. Добавьте группы через меню 'Группы'\n"
            "2. Создайте текст или используйте шаблоны\n"
            "3. Отправьте сообщение или настройте расписание\n\n"
            "Для связи с разработчиком: @your_username",
            reply_markup=get_main_menu_kb()
        )
    except Exception as e:
        logger.error(f"Ошибка в show_help: {e}")
        await callback_query.answer("⚠️ Произошла ошибка", show_alert=True)
    finally:
        await callback_query.answer()

@dp.callback_query(F.data == "settings_menu")
async def settings_menu(callback_query: types.CallbackQuery):
    try:
        await callback_query.message.edit_text(
            "⚙️ Настройки бота:",
            reply_markup=get_settings_menu_kb()
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в settings_menu: {e}")
    finally:
        await callback_query.answer()

# ======================
# ПЛАНИРОВЩИК
# ======================

async def check_scheduled_posts():
    while True:
        try:
            now = datetime.now(pytz.timezone(TIMEZONE)).strftime("%H:%M")
            posts = db.get_scheduled_posts()
            
            for post in posts:
                if post['send_time'] == now:
                    logger.info(f"Начинаю запланированную отправку в {len(post['groups'])} групп")
                    
                    success = 0
                    errors = 0
                    
                    for group in post['groups']:
                        if await send_to_group(group, post['text']):
                            success += 1
                        else:
                            errors += 1
                    
                    logger.info(f"Запланированная отправка завершена: успешно {success}, ошибок {errors}")
                    
                    if post.get('one_time', True):
                        db.deactivate_scheduled_post(post['id'])
            
            await asyncio.sleep(60)
        
        except Exception as e:
            logger.error(f"Ошибка в check_scheduled_posts: {e}")
            await asyncio.sleep(60)

# ======================
# ЗАПУСК БОТА
# ======================

async def on_startup():
    try:
        await bot.send_message(ADMIN_CHAT_ID, "🤖 Бот запущен и готов к работе!")
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}")

async def on_shutdown():
    try:
        await bot.send_message(ADMIN_CHAT_ID, "🛑 Бот остановлен!")
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}")

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    if "message is not modified" not in str(exception):
        logger.error(f"Ошибка при обработке {update}: {exception}")
    return True

async def main():
    try:
        await client.start(phone=PHONE_NUMBER)
        logger.info("Telegram клиент запущен")
        
        # Запуск фоновых задач
        asyncio.create_task(check_scheduled_posts())
        
        logger.info("Бот запущен")
        await dp.start_polling(
            bot,
            polling_timeout=30,
            relax=0.1,
            allowed_updates=dp.resolve_used_update_types(),
            on_startup=on_startup,
            on_shutdown=on_shutdown
        )
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        await client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())