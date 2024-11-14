import sqlite3
import random
from datetime import datetime, timedelta

# Подключение к базе данных (создаст файл futures_data_test.db, если его нет)
conn = sqlite3.connect('futures_data.db')
cursor = conn.cursor()

# Удаление таблицы, если она существует
cursor.execute('DROP TABLE IF EXISTS futures_data')
conn.commit()
print("Таблица futures_data удалена, если она существовала.")

# Создание таблицы futures_data
cursor.execute('''
    CREATE TABLE futures_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market_type TEXT NOT NULL,
            last_price REAL,
            low_price REAL,
            high_price REAL,
            volume_24h_contracts REAL,
            volume_24h_base_currency REAL,
            turnover_24h_usd REAL,
            open_interest_contracts REAL,
            open_interest_base_currency REAL,
            contract_size REAL,
            contract_type TEXT,
            timestamp DATETIME
    );
''')
conn.commit()
print("Таблица futures_data создана или проверена.")


# Функция для генерации случайных данных
def generate_random_data(symbols, exchange_names, contract_types, num_records=100):
    data = []
    for _ in range(num_records):
        symbol = random.choice(symbols)
        exchange = random.choice(exchange_names)
        market_type = 'futures'
        last_price = round(random.uniform(1000, 50000), 2)
        volume_24h_contracts = round(random.uniform(100, 100000), 2)
        volume_24h_base_currency = round(volume_24h_contracts * random.uniform(0.1, 1), 2)
        turnover_24h_usd = round(volume_24h_contracts * last_price, 2)
        open_interest_contracts = round(random.uniform(100, 50000), 2)
        open_interest_base_currency = round(open_interest_contracts * random.uniform(0.1, 1), 2)
        low_price = round(random.uniform(0.01, 10), 2)
        high_price = round(random.uniform(0.01, 10), 2)
        contract_size = round(random.uniform(0.01, 10), 2)
        contract_type = random.choice(contract_types)

        # Преобразование timestamp в ISO строку
        timestamp = (datetime.now() - timedelta(hours=random.randint(0, 24))).isoformat()

        data.append((symbol, exchange, market_type, last_price, volume_24h_contracts, volume_24h_base_currency,
                     turnover_24h_usd, open_interest_contracts, low_price, high_price,  open_interest_base_currency, contract_size,
                     contract_type, timestamp))
    return data


# Параметры для генерации данных
symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
exchange_names = ['OKX']
contract_types = ['perpetual', 'quarterly', 'biquarterly']

# Генерация и вставка случайных данных

dummy_data = generate_random_data(symbols, exchange_names, contract_types, num_records=100)

cursor.executemany('''
    INSERT INTO futures_data (
        symbol, exchange, market_type, last_price, low_price, high_price, volume_24h_contracts, 
        volume_24h_base_currency, turnover_24h_usd, open_interest_contracts, 
        open_interest_base_currency, contract_size, contract_type, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', dummy_data)


# Сохранение изменений и закрытие соединения
conn.commit()
print("Случайные данные успешно добавлены в futures_data.")
conn.close()
