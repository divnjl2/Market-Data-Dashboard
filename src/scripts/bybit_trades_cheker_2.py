import os
import shutil
import sqlite3
import logging
import requests
import threading
import schedule  # добавляем schedule для управления расписанием
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("analyze_trades.log"),
        logging.StreamHandler()
    ]
)

# Пути к базам данных
bybit_trades_db_path = 'bybit_trades.db'  # База данных с исходными сделками
bybit_analysis_db_path = 'bybit_analysis.db'  # База данных анализа

# Подключение к базе данных для сохранения анализа
conn_analysis = sqlite3.connect(bybit_analysis_db_path)
cursor_analysis = conn_analysis.cursor()

# Создание таблицы для хранения результатов анализа (если она еще не создана)
cursor_analysis.execute('''
CREATE TABLE IF NOT EXISTS trade_analysis (
    symbol TEXT,
    market_type TEXT,
    exchange TEXT NOT NULL,
    date TEXT,
    total_trades INTEGER,
    total_volume REAL
)
''')
conn_analysis.commit()


# Проверка и добавление колонок, если необходимо
def add_missing_columns(cursor):
    # Получение списка существующих колонок
    cursor.execute("PRAGMA table_info(trade_analysis);")
    columns = [col[1] for col in cursor.fetchall()]

    # Добавление колонки 'market_type', если отсутствует
    if 'market_type' not in columns:
        cursor.execute("ALTER TABLE trade_analysis ADD COLUMN market_type TEXT")
        logging.info("Колонка 'market_type' добавлена.")
    else:
        logging.info("Колонка 'market_type' уже существует.")

    # Добавление колонки 'updated_at', если отсутствует
    if 'updated_at' not in columns:
        cursor.execute("ALTER TABLE trade_analysis ADD COLUMN updated_at TIMESTAMP")
        cursor.execute("UPDATE trade_analysis SET updated_at = CURRENT_TIMESTAMP")
        logging.info("Колонка 'updated_at' добавлена.")
    else:
        logging.info("Колонка 'updated_at' уже существует.")

    # Добавление колонки 'exchange', если отсутствует
    if 'exchange' not in columns:
        cursor.execute("ALTER TABLE trade_analysis ADD COLUMN exchange TEXT DEFAULT 'Bybit'")
        logging.info("Колонка 'exchange' добавлена с биржей по умолчанию 'Bybit'.")
    else:
        logging.info("Колонка 'exchange' уже существует.")


# Добавление недостающих колонок в основной базе данных
add_missing_columns(cursor_analysis)


# Функция для получения всех торгуемых символов на указанном рынке (спот или фьючерсы) через Bybit API
def get_all_symbols(category):
    url = f"https://api.bybit.com/v5/market/tickers?category={category}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        symbols = [ticker['symbol'] for ticker in data['result']['list']]
        logging.info(f"Получены символы для {category}: {symbols}")
        return symbols
    else:
        logging.error(f"Не удалось получить символы для {category}, статус код: {response.status_code}")
        return []


# Функция для анализа сделок и записи результатов в базу данных
def analyze_trades(symbol, market_type, cursor_analysis_thread, cursor_trades_thread):
    trades = get_unique_trades(symbol, market_type, cursor_trades_thread)

    if not trades:
        logging.info(f"Нет сделок для символа {symbol} на рынке {market_type}.")
        return

    total_trades = len(trades)
    total_volume = sum([trade[3] for trade in trades])

    cursor_analysis_thread.execute('''
        INSERT INTO trade_analysis (symbol, market_type, exchange, date, total_trades, total_volume, updated_at)
        VALUES (?, ?, 'Bybit', ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (
        symbol,
        market_type,
        datetime.now().strftime("%Y-%m-%d"),
        total_trades,
        total_volume
    ))
    cursor_analysis_thread.connection.commit()

    logging.info(f"Символ: {symbol}, Рынок: {market_type}, Биржа: Bybit, Сделки: {total_trades}, Объем: {total_volume}")


# Функция для получения уникальных сделок по символу
def get_unique_trades(symbol, market_type, cursor_trades_thread):
    query = '''
        SELECT DISTINCT execId, symbol, price, qty, side, timestamp 
        FROM trades 
        WHERE symbol = ? AND market_type = ? 
        ORDER BY timestamp, execId
    '''
    cursor_trades_thread.execute(query, (symbol, market_type))
    return cursor_trades_thread.fetchall()


# Основная функция для запуска анализа по всем символам на указанных рынках
def analyze_all_symbols(cursor_analysis_thread, cursor_trades_thread):
    spot_symbols = get_all_symbols("spot")
    futures_symbols = get_all_symbols("linear")

    if not spot_symbols and not futures_symbols:
        logging.error("Не удалось получить список символов ни для спотового, ни для фьючерсного рынка.")
        return

    for symbol in spot_symbols:
        logging.info(f"Начало анализа для символа {symbol} (спот)")
        analyze_trades(symbol, "spot", cursor_analysis_thread, cursor_trades_thread)

    for symbol in futures_symbols:
        logging.info(f"Начало анализа для символа {symbol} (фьючерсы)")
        analyze_trades(symbol, "futures", cursor_analysis_thread, cursor_trades_thread)


# Функция для фонового обновления данных
def background_update():
    logging.info("Фоновое обновление данных началось.")

    # Создаём отдельное соединение для фонового потока для анализа и торговли
    conn_analysis_thread = sqlite3.connect(bybit_analysis_db_path)
    cursor_analysis_thread = conn_analysis_thread.cursor()

    conn_trades_thread = sqlite3.connect(bybit_trades_db_path)
    cursor_trades_thread = conn_trades_thread.cursor()

    # Добавление недостающих колонок в потоке
    add_missing_columns(cursor_analysis_thread)

    # Запуск анализа с новым соединением
    analyze_all_symbols(cursor_analysis_thread, cursor_trades_thread)

    # Закрытие соединения после завершения анализа
    conn_analysis_thread.close()
    conn_trades_thread.close()
    logging.info("Фоновое обновление данных завершено.")


# Функция для отображения текущих данных из базы данных
def display_current_data():
    cursor_analysis.execute("SELECT * FROM trade_analysis")
    records = cursor_analysis.fetchall()
    for record in records:
        logging.info(
            f"Символ: {record[0]}, Рынок: {record[1]}, Дата: {record[2]}, Сделки: {record[3]}, Объем: {record[4]}")


# Основная функция для отображения данных и запуска фонового обновления
def main():
    # Отображаем текущие архивные данные пользователю
    display_current_data()

    # Запускаем фоновое обновление данных
    update_thread = threading.Thread(target=background_update)
    update_thread.start()

    # Проверяем статус обновления
    update_thread.join()
    logging.info("Данные обновлены и готовы к просмотру.")


# Запуск анализа
if __name__ == "__main__":
    main()

    # Закрытие подключений к базе данных
    conn_analysis.close()

# Планировщик для запуска main() каждый час
def scheduled_job():
    logging.info("Запуск фоновой задачи по расписанию.")
    main()

if __name__ == "__main__":
    # Запускаем задачу обновления данных каждый час
    schedule.every().hour.do(scheduled_job)  # Настройка расписания на каждый час

    # Бесконечный цикл, чтобы планировщик выполнял задачи
    while True:
        schedule.run_pending()  # Выполняет запланированные задачи, если время подошло
        time.sleep(1)  # Проверка каждую секунду
