import requests
import sqlite3
import logging
from decimal import Decimal, InvalidOperation

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)




def create_db():
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()


    # Создаем таблицу для хранения данных
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market_type TEXT NOT NULL,  -- 'spot' или 'futures'
            last_price TEXT,
            volume_24h TEXT,
            options TEXT,
            strike_price TEXT,
            option_type TEXT,  -- 'Call' или 'Put'
            expiry_date TEXT,
            price_usdt TEXT,
            high_price_24h TEXT,
            low_price_24h TEXT,
            trades_24h TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    conn.close()


def save_to_db(data, exchange, market_type):
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()

    # Логирование получения данных
    logging.info(f"Сохранение данных для {exchange} ({market_type}): {data[:5]}...")  # Логируем первые 5 записей

    # Вставляем данные
    for item in data:
        try:
            symbol = item.get('symbol') if exchange != 'OKX' else item.get('instId')
            strike_price = item.get('strikePrice')
            option_type = item.get('side')  # 'Call' или 'Put'
            expiry_date = item.get('expiryDate')

            # Используем Decimal для точного представления чисел
            last_price = Decimal(str(item.get('lastPrice') or item.get('last') or 0))
            volume_24h = Decimal(str(item.get('volume24h') or item.get('turnover24h') or item.get('vol24h') or 0))
            high_price_24h = Decimal(str(item.get('highPrice24h') or item.get('high24h') or 0))
            low_price_24h = Decimal(str(item.get('lowPrice24h') or item.get('low24h') or 0))
            options = Decimal(str(item.get('lowPrice24h') or item.get('low24h') or 0))

            # Расчет trades_24h как volume_24h / last_price, если значение отсутствует
            trades_24h = Decimal(str(
                item.get('trades_24h') or
                (volume_24h / last_price if last_price > 0 else 0)
            ))

            # Расчет price_usdt как объем * последняя цена
            price_usdt = volume_24h * last_price

            # Форматируем числа без научной нотации
            last_price_str = format(last_price, 'f')
            volume_24h_str = format(volume_24h, 'f')
            price_usdt_str = format(price_usdt, 'f')
            high_price_24h_str = format(high_price_24h, 'f')
            low_price_24h_str = format(low_price_24h, 'f')
            trades_24h_str = format(trades_24h, 'f')

            cursor.execute('''
                INSERT INTO market_data (symbol, exchange, market_type, last_price, volume_24h, options, price_usdt,
                                         high_price_24h, low_price_24h, trades_24h, strike_price, option_type, expiry_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, exchange, market_type, last_price_str, volume_24h_str, symbol, price_usdt_str,
                  high_price_24h_str, low_price_24h_str, trades_24h_str, strike_price, option_type, expiry_date))

        except (InvalidOperation, TypeError, ValueError) as e:
            logging.error(f"Ошибка при обработке данных для {symbol}: {e}")
            continue

    conn.commit()
    conn.close()
    logging.info(f"Данные успешно сохранены для {market_type} с биржи {exchange}.")


# Получение данных с Binance для спотового рынка
def get_binance_spot_data():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    logging.info("Запрос данных с Binance (спотовый рынок)...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    logging.info(f"Получено {len(data)} инструментов с Binance (спотовый рынок).")  # Логируем количество инструментов

    # Корректное извлечение данных с учетом всех ключей
    for item in data:
        item['highPrice24h'] = float(item.get('highPrice', 0) or 0)
        item['lowPrice24h'] = float(item.get('lowPrice', 0) or 0)
        item['volume24h'] = float(item.get('volume', 0) or 0)
        item['lastPrice'] = float(item.get('lastPrice', 0) or 0)
        item['count'] = int(float(item.get('count', 0)) or 0)

    logging.info(f"Данные с Binance (спотовый рынок) успешно получены: {data[:5]}...")
    return data


# Получение данных с Binance для фьючерсного рынка
def get_binance_futures_data():
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    logging.info("Запрос данных с Binance (фьючерсный рынок)...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    logging.info(f"Получено {len(data)} инструментов с Binance (фьючерсный рынок).")  # Логируем количество инструментов

    # Корректное извлечение данных с учетом всех ключей
    for item in data:
        item['highPrice24h'] = float(item.get('highPrice', 0) or 0)
        item['lowPrice24h'] = float(item.get('lowPrice', 0) or 0)
        item['volume24h'] = float(item.get('volume', 0) or 0)
        item['lastPrice'] = float(item.get('lastPrice', 0) or 0)
        item['count'] = int(float(item.get('count', 0)) or 0)

    logging.info(f"Данные с Binance (фьючерсный рынок) успешно получены: {data[:5]}...")
    return data




# Получение данных с Bybit для спотового рынка
def get_bybit_spot_data():
    urls = [
        "https://api.bybit.com/v5/market/tickers?category=spot",
        "https://api.bybit.com/v5/market/tickers?category=linear",
        "https://api.bybit.com/v5/market/tickers?category=inverse"
    ]
    data = []
    for url in urls:
        logging.info(f"Запрос данных с Bybit ({url})...")
        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('result', {}).get('list', [])
        for item in result:
            # Рассчитываем trades_24h как volume_24h / last_price
            last_price = float(item.get('last', 1) or 1)
            volume_24h = float(item.get('turnover24h', item.get('vol24h', 0)) or 0)
            item['trades_24h'] = int(volume_24h / last_price) if last_price > 0 else 0
        data.extend(result)
        logging.info(f"Получено {len(result)} инструментов с Bybit по адресу {url}.")

    logging.info(f"Всего получено {len(data)} инструментов с Bybit.")
    return data


# Получение данных с Bybit для фьючерсного рынка
def get_bybit_futures_data():
    urls = [
        "https://api.bybit.com/v5/market/tickers?category=linear",
        "https://api.bybit.com/v5/market/tickers?category=inverse"
    ]
    data = []
    for url in urls:
        logging.info(f"Запрос данных с Bybit (фьючерсный рынок) ({url})...")
        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('result', {}).get('list', [])
        for item in result:
            # Рассчитываем trades_24h как volume_24h / last_price
            last_price = float(item.get('last', 1) or 1)
            volume_24h = float(item.get('turnover24h', item.get('vol24h', 0)) or 0)
            item['trades_24h'] = int(volume_24h / last_price) if last_price > 0 else 0
        data.extend(result)
        logging.info(f"Получено {len(result)} инструментов с Bybit по адресу {url}.")

    logging.info(f"Всего получено {len(data)} инструментов с Bybit (фьючерсный рынок).")
    return data

def get_okx_spot_data():
    urls = [
        "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
        "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    ]
    data = []
    for url in urls:
        logging.info(f"Запрос данных с OKX ({url})...")
        response = requests.get(url)
        response.raise_for_status()
        result = response.json()['data']
        for item in result:
            # Рассчитываем trades_24h как volume_24h / last_price
            last_price = float(item.get('last', 1) or 1)
            volume_24h = float(item.get('vol24h', 0) or 0)
            item['trades_24h'] = int(volume_24h / last_price) if last_price > 0 else 0
        data.extend(result)
        logging.info(f"Получено {len(result)} инструментов с OKX по адресу {url}.")

    logging.info(f"Всего получено {len(data)} инструментов с OKX.")
    return data


def get_okx_futures_data():
    urls = [
        "https://www.okx.com/api/v5/market/tickers?instType=FUTURES",
        "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    ]

    data = []
    for url in urls:
        logging.info(f"Запрос данных с OKX по адресу {url}...")
        response = requests.get(url)
        response.raise_for_status()
        result = response.json()['data']
        for item in result:
            # Рассчитываем trades_24h как volume_24h / last_price
            last_price = float(item.get('last', 1) or 1)  # Избегаем деления на ноль
            volume_24h = float(item.get('volCcy24h', item.get('vol24h', 0)) or 0)
            item['trades_24h'] = int(volume_24h / last_price) if last_price > 0 else 0
        data.extend(result)
        logging.info(f"Получено {len(result)} инструментов с OKX по адресу {url}.")

    logging.info(f"Всего получено {len(data)} инструментов с OKX.")
    return data

# Основной процесс для объединения данных со спотового и фьючерсного рынков с Binance, Bybit и OKX
def main():
    create_db()

    try:
        # Получение и сохранение данных с Binance (спотовый рынок)
        binance_spot_data = get_binance_spot_data()
        save_to_db(binance_spot_data, 'Binance', 'spot')

        # Получение и сохранение данных с Binance (фьючерсный рынок)
        binance_futures_data = get_binance_futures_data()
        save_to_db(binance_futures_data, 'Binance', 'futures')



        # Получение и сохранение данных с Bybit (спотовый рынок)
        bybit_spot_data = get_bybit_spot_data()
        save_to_db(bybit_spot_data, 'Bybit', 'spot')

        # Получение и сохранение данных с Bybit (фьючерсный рынок)
        bybit_futures_data = get_bybit_futures_data()
        save_to_db(bybit_futures_data, 'Bybit', 'futures')

        # Получение и сохранение данных с OKX (спотовый рынок)
        okx_spot_data = get_okx_spot_data()
        save_to_db(okx_spot_data, 'OKX', 'spot')

        # Получение и сохранение данных с OKX (фьючерсный рынок)
        okx_futures_data = get_okx_futures_data()
        save_to_db(okx_futures_data, 'OKX', 'futures')

        logging.info("Основной процесс завершен успешно.")
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()