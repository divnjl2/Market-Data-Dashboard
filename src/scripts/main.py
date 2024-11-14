import requests
import sqlite3
import logging
from decimal import Decimal, InvalidOperation
import time
import requests
import logging
from datetime import datetime, timedelta
import os
import threading

import sys
import locale


# Устанавливаем локаль для корректной обработки русского языка
locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def create_db():
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()

    # Создаем таблицу для хранения данных, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market_type TEXT NOT NULL,  -- 'spot' или 'futures'
            last_price REAL,
            volume_24h REAL,
            price_usdt REAL,
            high_price_24h REAL,
            low_price_24h REAL,
            trades_24h INTEGER,
            timestamp DATETIME
        );
    ''')

    # Устанавливаем уникальный индекс для предотвращения дублирования
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_market_data_unique
        ON market_data (symbol, exchange, market_type);
    ''')

    conn.commit()
    conn.close()



def save_to_db(data, exchange, market_type):
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()

    for item in data:
        try:
            symbol = item.get('symbol') if exchange != 'OKX' else item.get('instId')
            last_price = float(item.get('lastPrice') or item.get('last') or 0)
            volume_24h = float(item.get('volume24h') or item.get('turnover24h') or item.get('vol24h') or 0)
            price_usdt = float(item.get('price_usdt') or 0)
            high_price_24h = float(item.get('highPrice24h') or item.get('high24h') or 0)
            low_price_24h = float(item.get('lowPrice24h') or item.get('low24h') or 0)
            trades_24h = int(item.get('count') or 0)
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            updated_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # Время обновления

            # Выполняем вставку или обновление записи
            cursor.execute('''
                INSERT INTO market_data (symbol, exchange, market_type, last_price, volume_24h,
                                         price_usdt, high_price_24h, low_price_24h, trades_24h, timestamp, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, exchange, market_type) DO UPDATE SET
                    last_price = excluded.last_price,
                    volume_24h = excluded.volume_24h,
                    price_usdt = excluded.price_usdt,
                    high_price_24h = excluded.high_price_24h,
                    low_price_24h = excluded.low_price_24h,
                    trades_24h = excluded.trades_24h,
                    timestamp = excluded.timestamp,
                    updated_time = excluded.updated_time
            ''', (symbol, exchange, market_type, last_price, volume_24h,
                  price_usdt, high_price_24h, low_price_24h, trades_24h, timestamp, updated_time))

        except Exception as e:
            logging.error(f"Error processing data for {symbol}: {e}", exc_info=True)
            continue

    conn.commit()
    conn.close()



def add_updated_time_column():
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()

    # Добавляем колонку updated_time, если её нет
    try:
        cursor.execute("ALTER TABLE market_data ADD COLUMN updated_time DATETIME")
        logging.info("Колонка 'updated_time' успешно добавлена.")
    except sqlite3.OperationalError:
        logging.info("Колонка 'updated_time' уже существует.")

    conn.commit()
    conn.close()


def remove_duplicates():
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()

    logging.info("Удаление дублирующихся записей из базы данных...")
    try:
        cursor.execute('''
            DELETE FROM market_data
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM market_data
                GROUP BY symbol, exchange, market_type
            );
        ''')
        conn.commit()
        logging.info("Дублирующиеся записи успешно удалены.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при удалении дубликатов: {e}", exc_info=True)
    finally:
        conn.close()




def get_binance_spot_data():
    # Получаем информацию о символах для маппинга базовых и котируемых валют
    exchange_info_url = "https://api.binance.com/api/v3/exchangeInfo"
    logging.info("Получение информации о символах с Binance...")
    response = requests.get(exchange_info_url)
    response.raise_for_status()
    exchange_info = response.json()
    symbol_info = {}
    for s in exchange_info['symbols']:
        symbol_info[s['symbol']] = {'baseAsset': s['baseAsset'], 'quoteAsset': s['quoteAsset']}

    # Получаем данные 24-часового тикера
    url = "https://api.binance.com/api/v3/ticker/24hr"
    logging.info("Запрос данных с Binance (спотовый рынок)...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    logging.info(f"Получено {len(data)} инструментов с Binance (спотовый рынок).")

    # Функция для извлечения базовой и котируемой валют из символа
    def extract_assets(symbol):
        known_quote_assets = ['USDT', 'BTC', 'ETH', 'BNB', 'BUSD', 'EUR', 'TRY', 'BIDR', 'RUB', 'AUD', 'BRL', 'GBP',
                              'TUSD', 'DAI', 'IDRT']
        for quote_asset in known_quote_assets:
            if symbol.endswith(quote_asset):
                base_asset = symbol[:-len(quote_asset)]
                return base_asset, quote_asset
        return None, None

    # Функция для расчета цены в USDT для кросс-курсов с учетом объемов
    def calculate_cross_pair_price(baseAsset, quoteAsset, item, data):
        # Найдем цену для базовой и котируемой валюты в USDT
        base_to_usdt = next((float(i['lastPrice']) for i in data if i['symbol'] == f"{baseAsset}USDT"), None)
        quote_to_usdt = next((float(i['lastPrice']) for i in data if i['symbol'] == f"{quoteAsset}USDT"), None)

        if base_to_usdt and quote_to_usdt:
            # Рассчитываем цену кросс-курса в USDT через промежуточные пары
            cross_price_in_usdt = base_to_usdt / quote_to_usdt  # Это цена 1 базовой валюты в USDT
            return cross_price_in_usdt * item['volume24h']  # Умножаем на объем, чтобы получить сумму в USDT
        return 0.0  # Если не удалось найти цену, возвращаем 0

    # Обрабатываем данные
    for item in data:
        symbol = item['symbol']
        if symbol in symbol_info:
            baseAsset = symbol_info[symbol]['baseAsset']
            quoteAsset = symbol_info[symbol]['quoteAsset']
        else:
            logging.warning(
                f"Символ {symbol} не найден в exchangeInfo, пытаемся извлечь базовую и котируемую валюты из символа.")
            baseAsset, quoteAsset = extract_assets(symbol)
            if baseAsset is None or quoteAsset is None:
                logging.warning(
                    f"Не удалось определить базовую и котируемую валюты для символа {symbol}, пропускаем его.")
                continue  # Пропускаем символ, если не удалось определить валюты
        item['baseAsset'] = baseAsset
        item['quoteAsset'] = quoteAsset

        # Извлекаем остальные поля
        item['highPrice24h'] = float(item.get('highPrice', 0) or 0)
        item['lowPrice24h'] = float(item.get('lowPrice', 0) or 0)
        item['volume24h'] = float(item.get('volume', 0) or 0)  # Объем в базовой валюте
        item['quoteVolume24h'] = float(item.get('quoteVolume', 0) or 0)  # Объем в котируемой валюте
        item['lastPrice'] = float(item.get('lastPrice', 0) or 0)
        item['count'] = int(float(item.get('count', 0)) or 0)  # Количество сделок за 24 часа

        # Логика расчета цены в USDT
        if item['quoteAsset'] == 'USDT':
            # Котируемая валюта — USDT, объем в USDT уже известен
            item['price_usdt'] = item['quoteVolume24h']
        elif item['baseAsset'] == 'USDT':
            # Базовая валюта — USDT, объем в базовой валюте
            item['price_usdt'] = item['volume24h']
        else:
            # Если это кросс-курс (например, ETHBTC), рассчитываем через промежуточные пары
            item['price_usdt'] = calculate_cross_pair_price(baseAsset, quoteAsset, item, data)

    logging.info(f"Данные с Binance (спотовый рынок) успешно получены: {data[:5]}...")
    return data


# Получение данных с Binance для фьючерсного рынка
def get_binance_futures_data():
    # Получаем информацию о символах для маппинга базовых и котируемых валют
    exchange_info_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    logging.info("Получение информации о символах с Binance (фьючерсный рынок)...")
    response = requests.get(exchange_info_url)
    response.raise_for_status()
    exchange_info = response.json()
    symbol_info = {}
    for s in exchange_info['symbols']:
        symbol_info[s['symbol']] = {'baseAsset': s['baseAsset'], 'quoteAsset': s['quoteAsset']}

    # Получаем данные 24-часового тикера
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    logging.info("Запрос данных с Binance (фьючерсный рынок)...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    logging.info(f"Получено {len(data)} инструментов с Binance (фьючерсный рынок).")

    for item in data:
        symbol = item['symbol']
        if symbol in symbol_info:
            baseAsset = symbol_info[symbol]['baseAsset']
            quoteAsset = symbol_info[symbol]['quoteAsset']
        else:
            logging.warning(f"Символ {symbol} не найден в exchangeInfo, пропускаем его.")
            continue  # Пропускаем символ, если нет информации

        item['baseAsset'] = baseAsset
        item['quoteAsset'] = quoteAsset

        item['highPrice24h'] = float(item.get('highPrice', 0) or 0)
        item['lowPrice24h'] = float(item.get('lowPrice', 0) or 0)
        item['volume24h'] = float(item.get('volume', 0) or 0)  # Объем в базовой валюте
        item['quoteVolume24h'] = float(item.get('quoteVolume', 0) or 0)  # Объем в котируемой валюте
        item['lastPrice'] = float(item.get('lastPrice', 0) or 0)
        item['count'] = int(float(item.get('count', 0)) or 0)

        # Определяем price_usdt
        if item['quoteAsset'] == 'USDT':
            item['price_usdt'] = item['quoteVolume24h']
        elif item['baseAsset'] == 'USDT':
            item['price_usdt'] = item['volume24h']
        else:
            item['price_usdt'] = 0.0

    logging.info(f"Данные с Binance (фьючерсный рынок) успешно получены: {data[:5]}...")
    return data







def get_bybit_spot_data(binance_data=None):
    url = "https://api.bybit.com/v5/market/tickers?category=spot"
    logging.info(f"Запрос данных с Bybit (спотовый рынок) ({url})...")
    response = requests.get(url)
    response.raise_for_status()
    result = response.json().get('result', {}).get('list', [])
    data = []

    known_quote_currencies = ['USDT', 'BTC', 'ETH', 'USDC', 'DAI', 'BNB', 'BUSD']

    for item in result:
        symbol = item.get('symbol')
        base_currency, quote_currency = None, None

        # Извлекаем базовую и котируемую валюты
        for qc in known_quote_currencies:
            if symbol.endswith(qc):
                base_currency = symbol[:-len(qc)]
                quote_currency = qc
                break

        if base_currency is None or quote_currency is None:
            logging.warning(f"Не удалось определить базовую и котируемую валюты для символа {symbol}, пропускаем его.")
            continue

        item['baseAsset'] = base_currency
        item['quoteAsset'] = quote_currency
        item['lastPrice'] = float(item.get('lastPrice') or 0)
        item['volume24h'] = float(item.get('volume24h') or 0)
        item['quoteVolume24h'] = float(item.get('turnover24h') or 0)
        item['highPrice24h'] = float(item.get('highPrice24h') or 0)
        item['lowPrice24h'] = float(item.get('lowPrice24h') or 0)

        # Устанавливаем количество сделок в 0, так как запросы о трейдах исключены
        item['count'] = 0

        # Определяем цену в USDT
        if quote_currency == 'USDT':
            item['price_usdt'] = item['quoteVolume24h']
        elif base_currency == 'USDT':
            item['price_usdt'] = item['volume24h']
        else:
            item['price_usdt'] = 0.0

        data.append(item)

    logging.info(f"Получено {len(data)} инструментов с Bybit (спотовый рынок).")
    return data


def get_bybit_futures_data(binance_data=None):
    categories = ['linear', 'inverse']
    data = []

    for category in categories:
        url = f"https://api.bybit.com/v5/market/tickers?category={category}"
        logging.info(f"Запрос данных с Bybit (фьючерсный рынок) ({category}) ({url})...")

        # Запрос данных
        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('result', {}).get('list', [])

        known_quote_currencies = ['USDT', 'BTC', 'ETH', 'USDC', 'DAI', 'BNB', 'BUSD']

        for item in result:
            symbol = item.get('symbol')

            # Извлекаем базовую и котируемую валюты из символа
            base_currency, quote_currency = None, None
            for qc in known_quote_currencies:
                if symbol.endswith(qc):
                    base_currency = symbol[:-len(qc)]
                    quote_currency = qc
                    break

            if base_currency is None or quote_currency is None:
                logging.warning(
                    f"Не удалось определить базовую и котируемую валюты для символа {symbol}, пропускаем его.")
                continue

            item['baseAsset'] = base_currency
            item['quoteAsset'] = quote_currency

            # Извлекаем необходимые поля
            item['lastPrice'] = float(item.get('lastPrice') or 0)
            item['volume24h'] = float(item.get('volume24h') or 0)
            item['quoteVolume24h'] = float(item.get('turnover24h') or 0)
            item['highPrice24h'] = float(item.get('highPrice24h') or 0)
            item['lowPrice24h'] = float(item.get('lowPrice24h') or 0)

            # Оценка количества сделок на основе объема Bybit и данных Binance
            item['count'] = 0

            # Определяем price_usdt
            if category == 'linear' and quote_currency == 'USDT':
                # Для USDT-маржинальных контрактов котируемая валюта — USDT
                item['price_usdt'] = item['quoteVolume24h']
            elif category == 'inverse' and base_currency == 'USDT':
                # Для коин-маржинальных контрактов базовая валюта — USDT
                item['price_usdt'] = item['volume24h']
            else:
                # Ни базовая, ни котируемая валюта не являются USDT
                item['price_usdt'] = 0.0

            data.append(item)

        logging.info(f"Получено {len(result)} инструментов с Bybit по категории {category}.")

    logging.info(f"Всего получено {len(data)} инструментов с Bybit (фьючерсный рынок).")
    return data


# Функция для сопоставления символов между Bybit и Binance
def map_symbol_bybit_to_binance(symbol):
    # Убираем слеши или другие символы, если они отличаются
    symbol = symbol.replace('/', '')
    # Добавьте логику для изменения символов, если они отличаются между биржами
    return symbol


def get_okx_spot_data():
    # Получаем информацию о символах для маппинга базовых и котируемых валют
    exchange_info_url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
    logging.info("Получение информации о символах с OKX...")

    response = requests.get(exchange_info_url)
    response.raise_for_status()  # Проверяем, что запрос прошел успешно
    exchange_info = response.json().get('data', [])

    symbol_info = {}
    for s in exchange_info:
        symbol_info[s['instId']] = {'baseAsset': s['baseCcy'], 'quoteAsset': s['quoteCcy']}

    # Функция для извлечения базовой и котируемой валют из символа
    def extract_assets(symbol):
        known_quote_assets = ['USDT', 'BTC', 'ETH', 'BNB', 'BUSD', 'EUR', 'TRY', 'BIDR', 'RUB', 'AUD', 'BRL', 'GBP',
                              'TUSD', 'DAI', 'IDRT']
        for quote_asset in known_quote_assets:
            if symbol.endswith(quote_asset):
                base_asset = symbol[:-len(quote_asset)]
                return base_asset, quote_asset
        return None, None

    # Функция для расчета цены в USDT для кросс-курсов с учетом объемов
    def calculate_cross_pair_price(baseAsset, quoteAsset, item, data):
        # Найдем цену для базовой и котируемой валюты в USDT
        base_to_usdt = next((float(i['last']) for i in data if i['instId'] == f"{baseAsset}-USDT"), None)
        quote_to_usdt = next((float(i['last']) for i in data if i['instId'] == f"{quoteAsset}-USDT"), None)

        if base_to_usdt and quote_to_usdt:
            # Рассчитываем цену кросс-курса в USDT через промежуточные пары
            cross_price_in_usdt = base_to_usdt / quote_to_usdt  # Это цена 1 базовой валюты в USDT
            return cross_price_in_usdt * item['volume24h']  # Умножаем на объем, чтобы получить сумму в USDT
        return 0.0  # Если не удалось найти цену, возвращаем 0

    # Получаем данные 24-часового тикера
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    logging.info("Запрос данных с OKX (спотовый рынок)...")

    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Ошибка при запросе данных: {response.status_code}")
        return []

    data = response.json().get('data', [])
    logging.info(f"Получено {len(data)} инструментов с OKX (спотовый рынок).")

    # Обрабатываем данные
    for item in data:
        symbol = item['instId']
        if symbol in symbol_info:
            baseAsset = symbol_info[symbol]['baseAsset']
            quoteAsset = symbol_info[symbol]['quoteAsset']
        else:
            logging.warning(
                f"Символ {symbol} не найден в exchangeInfo, пытаемся извлечь базовую и котируемую валюты из символа.")
            baseAsset, quoteAsset = extract_assets(symbol)
            if baseAsset is None or quoteAsset is None:
                logging.warning(
                    f"Не удалось определить базовую и котируемую валюты для символа {symbol}, пропускаем его.")
                continue  # Пропускаем символ, если не удалось определить валюты

        item['baseAsset'] = baseAsset
        item['quoteAsset'] = quoteAsset

        # Извлекаем остальные поля
        item['highPrice24h'] = float(item.get('high24h', 0) or 0)
        item['lowPrice24h'] = float(item.get('low24h', 0) or 0)
        item['volume24h'] = float(item.get('vol24h', 0) or 0)  # Объем в базовой валюте
        item['quoteVolume24h'] = float(item.get('volCcy24h', 0) or 0)  # Объем в котируемой валюте
        item['lastPrice'] = float(item.get('last', 0) or 0)

        # Логика расчета объема в USDT
        if item['quoteAsset'] == 'USDT':
            # Котируемая валюта — USDT, объем в USDT уже известен
            item['price_usdt'] = item['quoteVolume24h']
        elif item['baseAsset'] == 'USDT':
            # Базовая валюта — USDT, объем в базовой валюте
            item['price_usdt'] = item['volume24h']
        else:
            # Если это кросс-курс (например, ETHBTC), рассчитываем через промежуточные пары
            item['price_usdt'] = calculate_cross_pair_price(baseAsset, quoteAsset, item, data)

    logging.info(f"Данные с OKX (спотовый рынок) успешно получены: {data[:5]}...")
    return data





def fetch_data(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; OKXDataCollector/1.0)"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе к {url}: {e}")
        return []

def display_current_data():
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM market_data ORDER BY timestamp DESC LIMIT 5")  # Покажем последние 5 записей
    records = cursor.fetchall()
    for record in records:
        logging.info(f"Запись: {record}")
    conn.close()

def background_update():
    logging.info("Фоновое обновление данных началось.")

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

    # Удаление дубликатов после обновления
    remove_duplicates()

    logging.info("Фоновое обновление данных завершено.")



# Основной процесс для объединения данных со спотового и фьючерсного рынков с Binance, Bybit и OKX
def main():
    create_db()
    add_updated_time_column()  # Добавляем колонку updated_time, если её нет
    display_current_data()  # Отображаем текущие данные перед началом обновления
    update_thread = threading.Thread(target=background_update)
    update_thread.start()
    update_thread.join()  # Ожидаем завершения фонового обновления
    logging.info("Фоновое обновление данных завершено.")

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


        # Удаление дубликатов из базы данных
        remove_duplicates()

    except Exception as e:
        logging.error(f"Произошла ошибка во время выполнения основного процесса: {e}")

if __name__ == "__main__":
    main()
