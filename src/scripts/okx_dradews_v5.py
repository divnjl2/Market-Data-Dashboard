import aiohttp
import asyncio
import logging
from datetime import datetime
import time
import os
import json
import sqlite3
from more_itertools import chunked

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Конфигурация базы данных
DB_NAME = 'trades_data_okx.db'

import sys
import asyncio

# Принудительно установить SelectorEventLoop на Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# Конфигурация кэша
CACHE_DIR = "cache"

# Ограничение на количество запросов
SEMAPHORE_LIMIT = 10
semaphore = asyncio.BoundedSemaphore(SEMAPHORE_LIMIT)


# Функция для создания соединения с базой данных
def create_db_connection():
    # Проверка на существование файла базы данных и его создание
    if not os.path.exists(DB_NAME):
        open(DB_NAME, 'w').close()  # Создание пустого файла базы данных
    conn = sqlite3.connect(DB_NAME)
    return conn

# Функция для создания таблицы, если она еще не существует
def create_table():
    conn = create_db_connection()  # Подключаемся к базе данных
    cursor = conn.cursor()  # Создаем курсор для выполнения SQL-запросов

    # SQL-запрос для создания таблицы trades_data с полем trade_type
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

    conn.commit()  # Подтверждаем изменения
    conn.close()  # Закрываем соединение с базой данных

# Функция для проверки наличия дубликата записи
def is_duplicate_record(symbol, trade_type):
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        # Проверка на наличие записи с символом и типом сделки, упорядоченная по времени
        cursor.execute(
            'SELECT 1 FROM trades_data WHERE symbol = ? AND trade_type = ? ORDER BY timestamp DESC LIMIT 1',
            (symbol, trade_type)
        )
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except sqlite3.Error as e:
        logging.error(f"Ошибка при проверке дубликата для {symbol} ({trade_type}): {e}")
        return False

# Функция для сохранения данных о сделках
def save_trade_data(symbol, trade_type, total_trades, total_volume, official_volume):
    if is_duplicate_record(symbol, trade_type):
        logging.info(f"Запись для {symbol} ({trade_type}) уже существует, пропуск сохранения.")
        return
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        # Вставка данных в таблицу с учетом trade_type
        cursor.execute('''
            INSERT INTO trades_data (symbol, trade_type, total_trades, total_volume, official_volume, exchange)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (symbol, trade_type, total_trades, total_volume, official_volume, 'OKX'))
        conn.commit()
        conn.close()
        logging.info(f"Данные для {symbol} ({trade_type}) успешно сохранены в базу данных.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при записи данных для {symbol} ({trade_type}) в базу данных: {e}")


def save_trade_data(symbol, trade_type, total_trades, total_volume, official_volume):
    if is_duplicate_record(symbol):
        logging.info(f"Запись для {symbol} ({trade_type}) уже существует, пропуск сохранения.")
        return
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO trades_data (symbol, trade_type, total_trades, total_volume, official_volume, exchange) VALUES (?, ?, ?, ?, ?, ?)',
                       (symbol, trade_type, total_trades, total_volume, official_volume, 'OKX'))
        conn.commit()
        conn.close()
        logging.info(f"Данные для {symbol} ({trade_type}) успешно сохранены в базу данных.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при записи данных для {symbol} ({trade_type}) в базу данных: {e}")

# Кэш
def ensure_cache_dir_exists():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)


def cache_filename(symbol, start_time, end_time):
    start_time_str = datetime.utcfromtimestamp(start_time / 1000).strftime('%Y-%m-%d_%H-%M-%S')
    end_time_str = datetime.utcfromtimestamp(end_time / 1000).strftime('%Y-%m-%d_%H-%M-%S')
    return os.path.join(CACHE_DIR, f"{symbol}_{start_time_str}_to_{end_time_str}.json")


def is_cached(symbol, start_time, end_time):
    filename = cache_filename(symbol, start_time, end_time)
    return os.path.exists(filename)

def load_from_cache(symbol, start_time, end_time):
    filename = cache_filename(symbol, start_time, end_time)
    with open(filename, 'r') as file:
        return json.load(file)


def save_to_cache(symbol, start_time, end_time, data):
    ensure_cache_dir_exists()
    filename = cache_filename(symbol, start_time, end_time)
    with open(filename, 'w') as file:
        json.dump(data, file)


async def get_trades_from_cache_or_api(symbol, start_time, end_time, fetch_function):
    if is_cached(symbol, start_time, end_time):
        logging.info(f"Загружены данные из кэша для {symbol} с {start_time} по {end_time}")
        return load_from_cache(symbol, start_time, end_time)
    else:
        logging.info(f"Запрос данных из API для {symbol} с {start_time} по {end_time}")
        data = await fetch_function(symbol, start_time, end_time)
        save_to_cache(symbol, start_time, end_time, data)
        return data



# Функции для работы с API
async def fetch_all_instruments(session, inst_type="SPOT"):
    url = f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}"

    try:
        async with session.get(url) as response:
            if response.status == 200:
                instruments_data = await response.json()
                instruments = instruments_data.get('data', [])
                if instruments:
                    logging.info(f"Успешно получено {len(instruments)} инструментов ({inst_type}).")
                else:
                    logging.warning(f"Список инструментов пуст ({inst_type}).")

                return instruments
            else:
                logging.error(f"Ошибка при запросе списка инструментов ({inst_type}): {response.status}")
                return []
    except Exception as e:
        logging.error(f"Исключение при запросе инструментов ({inst_type}): {e}")
        return []


def filter_instruments(instruments, currency="USDT", field="settleCcy"):
    filtered_symbols = []

    for instrument in instruments:
        currency_value = instrument.get(field, "")
        symbol = instrument.get('instId', "")

        if currency_value == currency:
            filtered_symbols.append(symbol)

    if filtered_symbols:
        logging.info(f"Отфильтровано {len(filtered_symbols)} торговых пар.")
    else:
        logging.warning(f"Не найдено торговых пар с валютой {currency}.")

    return filtered_symbols


async def fetch_trades_from_api(session, symbol, start_time, end_time):
    url = f"https://www.okx.com/api/v5/market/history-trades?instId={symbol}&limit=100"
    all_trades = []
    after_trade_id = None
    has_more_data = True
    processed_trade_ids = set()
    valid_trades_count = 0
    invalid_trades_count = 0

    while has_more_data:
        paginated_url = url + (f"&after={after_trade_id}" if after_trade_id else "")
        try:
            async with session.get(paginated_url, timeout=4) as response:
                logging.info(f"Запрос данных для {symbol}. URL: {paginated_url}")

                if response.status == 200:
                    trades_response = await response.json()
                    trades_result = trades_response.get('data', [])

                    if not trades_result:
                        logging.info(f"Нет больше данных для {symbol}, завершаем.")
                        has_more_data = False
                        break

                    valid_trades = [
                        trade for trade in trades_result
                        if int(trade['ts']) >= start_time and trade['tradeId'] not in processed_trade_ids
                    ]
                    invalid_trades_count += len(trades_result) - len(valid_trades)
                    valid_trades_count += len(valid_trades)

                    all_trades.extend(valid_trades)
                    all_trades = sorted(all_trades, key=lambda x: int(x['ts']))
                    processed_trade_ids.update(trade['tradeId'] for trade in valid_trades)

                    if valid_trades:
                        last_trade = valid_trades[-1]
                        after_trade_id = last_trade['tradeId']
                        last_trade_time = datetime.utcfromtimestamp(int(last_trade['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        logging.info(f"{symbol}: Получено {len(valid_trades)} валидных сделок. Последний tradeId: {after_trade_id}, Время: {last_trade_time}")

                    invalid_ratio = invalid_trades_count / (valid_trades_count + invalid_trades_count)
                    logging.info(f"{symbol}: Процент невалидных сделок: {invalid_ratio:.2%}")

                    if not valid_trades:
                        has_more_data = False
                elif response.status == 429:
                    # Ошибка превышения лимита запросов, динамическая задержка
                    retry_after = int(response.headers.get('Retry-After', 1))  # По умолчанию задержка 1 секунда
                    logging.error(f"Превышен лимит запросов для {symbol}. Ожидание {retry_after} секунд.")
                    await asyncio.sleep(retry_after)
                else:
                    logging.error(f"Ошибка при запросе: {response.status}")

                    # Добавляем логирование ответа от API
                    try:
                        error_data = await response.json()
                        logging.error(f"Данные ошибки от API: {error_data}")
                    except Exception as e:
                        logging.error(f"Не удалось получить данные ошибки от API: {e}")

            await asyncio.sleep(0.9)  # Задержка между запросами
        except asyncio.TimeoutError:
            logging.error(f"Тайм-аут при запросе данных для {symbol}, повтор запроса.")

    return all_trades




async def get_official_volume(session, symbol):
    url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
    async with session.get(url) as response:
        if response.status == 200:
            ticker_data = await response.json()
            if 'data' in ticker_data and ticker_data['data']:
                official_volume = float(ticker_data['data'][0]['vol24h'])
                logging.info(f"Официальный объем за 24 часа для {symbol}: {official_volume}")
                return official_volume
            else:
                logging.warning(f"Данные для {symbol} отсутствуют в ответе API.")
                return 0.0
        else:
            logging.error(f"Ошибка при получении официального объема для {symbol}: {response.status}")
            return 0.0



async def fetch_data_for_symbol(session, symbol, start_time, end_time):
    trades = await get_trades_from_cache_or_api(symbol, start_time, end_time,
                                                lambda s, st, et: fetch_trades_from_api(session, s, st, et))

    total_volume = sum(float(trade['sz']) for trade in trades)
    trades_24h = len(trades)

    official_volume = await get_official_volume(session, symbol)

    return trades_24h, total_volume, official_volume

def scale_to_24h(trades_count, volume, hours_collected):
    """
    Масштабирует количество сделок и объём до 24 часов.
    :param trades_count: Количество сделок.
    :param volume: Общий объём сделок.
    :param hours_collected: Время в часах, за которое были собраны данные.
    :return: Масштабированное количество сделок и объём.
    """
    # Проверяем, чтобы время не было равно 0
    if hours_collected <= 0:
        raise ValueError("Время сбора данных должно быть положительным числом.")

    # Вычисляем коэффициент для масштабирования до 24 часов
    scale_factor = 24 / hours_collected

    # Масштабируем сделки и объём
    scaled_trades_count = int(trades_count * scale_factor)
    scaled_volume = volume * scale_factor

    return scaled_trades_count, scaled_volume

async def fetch_with_semaphore(symbol, session, start_time, end_time):
    async with semaphore:
        # Вычисляем количество часов между start_time и end_time
        hours_collected = (end_time - start_time) / (60 * 60 * 1000)  # Преобразуем миллисекунды в часы

        # Проверяем кэш перед выполнением запроса
        if is_cached(symbol, start_time, end_time):
            logging.info(f"Загружены данные из кэша для {symbol} с {start_time} по {end_time}")
            data = load_from_cache(symbol, start_time, end_time)
            total_trades = len(data)
            total_volume = sum(float(trade['sz']) for trade in data)
            official_volume = await get_official_volume(session, symbol)

            # Масштабируем данные
            total_trades, total_volume = scale_to_24h(total_trades, total_volume, hours_collected)
            logging.info(
                f"{symbol}: Масштабированное количество сделок: {total_trades}, Объем: {total_volume}, Официальный объем: {official_volume}")

            # Сохраняем данные
            save_trade_data(symbol, total_trades, total_volume, official_volume)
            return  # Прерываем выполнение функции, если данные были загружены из кэша

        # Если данных в кэше нет, продолжаем с запросом из API
        logging.info(f"Запрос данных для {symbol} с {start_time} по {end_time}")
        total_trades, total_volume, official_volume = await fetch_data_for_symbol(session, symbol, start_time, end_time)

        # Масштабируем данные
        total_trades, total_volume = scale_to_24h(total_trades, total_volume, hours_collected)
        logging.info(
            f"{symbol}: Масштабированное количество сделок: {total_trades}, Объем: {total_volume}, Официальный объем: {official_volume}")

        # Сохраняем данные
        save_trade_data(symbol, total_trades, total_volume, official_volume)


async def get_data_for_multiple_symbols(symbols):
    # Вычисляем время начала и конца один раз
    end_time = int(time.time() * 1000)
    start_time = end_time - 1 * 60 * 60 * 1000

    async with aiohttp.ClientSession() as session:
        for symbol_chunk in chunked(symbols, SEMAPHORE_LIMIT):
            tasks = []
            for symbol in symbol_chunk:
                # Проверяем кэш перед выполнением запроса
                if is_cached(symbol, start_time, end_time):
                    logging.info(f"Загружены данные из кэша для {symbol} с {start_time} по {end_time}")
                    data = load_from_cache(symbol, start_time, end_time)
                    trades_24h = len(data)
                    total_volume = sum(float(trade['sz']) for trade in data)
                    official_volume = await get_official_volume(session, symbol)
                    logging.info(f"{symbol}: Количество сделок: {trades_24h}, Объем: {total_volume}, Официальный объем: {official_volume}")
                    save_trade_data(symbol, trades_24h, total_volume, official_volume)
                else:
                    # Если кэша нет, добавляем задачу для получения данных
                    tasks.append(fetch_with_semaphore(symbol, session, start_time, end_time))

            # Выполняем все задачи, если они есть
            if tasks:
                await asyncio.gather(*tasks)



# Основная функция для получения символов и запуска программы
async def main(fetch_spot=True, fetch_futures=True, fetch_swap=True):
    async with aiohttp.ClientSession() as session:
        symbols = []

        # Если флаг fetch_spot установлен в True, собираем данные для спота
        if fetch_spot:
            logging.info("Начало сбора данных для спотовых инструментов (SPOT)")
            spot_instruments = await fetch_all_instruments(session, "SPOT")
            spot_symbols = filter_instruments(spot_instruments, currency="USDT", field="quoteCcy")
            logging.info(f"Найдено {len(spot_symbols)} спотовых символов.")
            symbols += spot_symbols

        # Если флаг fetch_futures установлен в True, собираем данные для фьючерсов
        if fetch_futures:
            logging.info("Начало сбора данных для фьючерсных инструментов (FUTURES)")
            futures_instruments = await fetch_all_instruments(session, "FUTURES")
            futures_symbols = filter_instruments(futures_instruments, currency="USDT", field="settleCcy")
            logging.info(f"Найдено {len(futures_symbols)} фьючерсных символов.")
            symbols += futures_symbols

        # Если флаг fetch_swap установлен в True, собираем данные для свопов
        if fetch_swap:
            logging.info("Начало сбора данных для свопов (SWAP)")
            swap_instruments = await fetch_all_instruments(session, "SWAP")
            swap_symbols = filter_instruments(swap_instruments, currency="USDT", field="settleCcy")
            logging.info(f"Найдено {len(swap_symbols)} свопов.")
            symbols += swap_symbols

        # Проверяем, если символы были собраны
        if not symbols:
            logging.error("Не удалось найти символы с расчетной валютой USDT, завершение работы.")
            return

        logging.info(f"Получены символы для обработки: {symbols}")

        # Запускаем сбор данных
        await get_data_for_multiple_symbols(symbols)

# Запуск программы с флагами
create_table()
asyncio.run(main(fetch_spot=True, fetch_futures=False, fetch_swap=False))  # Здесь можно изменить флаги для каждого типа данных

