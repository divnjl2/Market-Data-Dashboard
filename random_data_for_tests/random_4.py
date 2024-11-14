import sqlite3
import random
from datetime import datetime, timedelta

# Имя базы данных
DB_NAME = 'trades_data_okx.db'

# Подключение к базе данных и создание таблицы, если она не существует
def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades_data (
            symbol TEXT,                   -- Символ торговой пары
            trade_type TEXT,               -- Тип сделки (SPOT, FUTURES, SWAP)
            total_trades INT,              -- Количество сделок за 24 часа
            total_volume REAL,             -- Общий объем сделок
            official_volume REAL,          -- Официальный объем
            exchange TEXT DEFAULT 'OKX',   -- Биржа, по умолчанию 'OKX'
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP -- Временная метка создания записи
        )
    ''')
    conn.commit()
    conn.close()

# Функция для генерации случайных данных и их вставки в базу данных
def generate_random_data(num_records=50):
    trade_types = ['SPOT', 'FUTURES', 'SWAP']
    symbols = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT', 'ADAUSDT']  # Примеры символов
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for _ in range(num_records):
        symbol = random.choice(symbols)
        trade_type = random.choice(trade_types)
        total_trades = random.randint(1000, 100000)  # Случайное количество сделок
        total_volume = round(random.uniform(1000.0, 100000.0), 2)  # Случайный общий объем сделок
        official_volume = round(random.uniform(1000.0, 100000.0), 2)  # Случайный официальный объем
        timestamp = datetime.now() - timedelta(minutes=random.randint(0, 1440))  # Временная метка в пределах последних 24 часов

        # Вставка данных в таблицу
        cursor.execute('''
            INSERT INTO trades_data (symbol, trade_type, total_trades, total_volume, official_volume, exchange, timestamp)
            VALUES (?, ?, ?, ?, ?, 'OKX', ?)
        ''', (symbol, trade_type, total_trades, total_volume, official_volume, timestamp))

    conn.commit()
    conn.close()
    print(f"{num_records} записей случайных данных успешно добавлены в таблицу trades_data.")

# Создание таблицы и генерация данных
create_table()
generate_random_data(num_records=50)
