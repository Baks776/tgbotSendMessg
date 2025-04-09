# Telegram Broadcast Bot

Бот для управления рассылками в Telegram группах и каналах.

## Возможности

- Управление списком групп/каналов
- Создание и использование шаблонов сообщений
- Планирование отправки сообщений
- Фильтрация групп по тегам
- Статистика отправки

## Установка

1. Убедитесь, что у вас установлен Python 3.10
2. Установите зависимости: pip install -r requirements.txt
3. Откройте:
👉 https://my.telegram.org/auth

🔹 2. Войдите через свой аккаунт Telegram
Введите номер телефона (в формате +79991234567).

Получите код в Telegram и введите его.

🔹 3. Создайте новое приложение
После входа нажмите "API development tools".

Заполните форму:

App title — название вашего приложения (любое, например MyBot).

Short name — короткое имя (латинскими буквами, например my_bot).

URL (опционально) — можно оставить пустым или указать сайт.

Platform — выберите Desktop.

Description — описание (например, Telegram bot for automation).

🔹 4. Получите API_ID и API_HASH
После создания приложения вы увидите:

api_id — числовой идентификатор (например, 123456).

api_hash — длинная строка из букв и цифр (например, abcdef1234567890abcdef1234567890).

⚠️ Важно!
Никому не передавайте API_HASH — это как пароль от вашего приложения.

Эти данные нужны для авторизации в Telegram API (например, в Telethon или Pyrogram).

## Настройка прокси (опционально)

Если вам нужно использовать прокси, добавьте их в файл `.env` в формате:
Поддерживаемые типы: socks5, socks4, http

## Первый запуск

1. При первом запуске вам нужно будет авторизовать Telegram клиент:
   - Введите номер телефона
   - Введите код подтверждения, который придет в Telegram
2. После успешной авторизации бот будет готов к работе

## Использование

Отправьте команду `/start` или `/menu` боту, чтобы открыть главное меню.

### Основные разделы:

1. **Группы** - управление списком групп/каналов
2. **Контент** - создание и управление сообщениями
3. **Расписание** - планирование отправки сообщений
4. **Статистика** - просмотр статистики работы бота
5. **Настройки** - настройки бота

## Логирование

Все действия бота записываются в файл `bot.log`.

## Важные примечания

1. Боту нужно быть администратором в группах/каналах, куда он будет отправлять сообщения
2. Соблюдайте ограничения Telegram (не более 20 сообщений в минуту)
3. Для остановки бота используйте Ctrl+C в терминале