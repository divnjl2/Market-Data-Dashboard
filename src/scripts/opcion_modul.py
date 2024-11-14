import sqlite3
import logging
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime
import threading
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)


def create_db():
    conn = sqlite3.connect('opcion_data.db')
    cursor = conn.cursor()

    # Создаем таблицу для хранения данных с уникальным ограничением
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS opcion_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market_type TEXT NOT NULL,
            last_price REAL,
            volume_24h REAL,
            options REAL,
            strike_price REAL,
            option_type TEXT,
            expiry_date TEXT,
            exercise_price REAL,
            price_usdt REAL,
            high_price_24h REAL,
            low_price_24h REAL,
            trades_24h INTEGER,
            timestamp DATETIME,
            updated_time DATETIME,
            UNIQUE(symbol, exchange, market_type)
        );
    ''')
    conn.commit()
    conn.close()




# Получение данных с Binance об опционах
def get_binance_options_data():
    # URL для получения всех опционных тикеров на Binance
    url = "https://eapi.binance.com/eapi/v1/ticker"

    logging.info(f"Запрос данных обо всех опционах с Binance по адресу {url}...")

    try:
        response = requests.get(url)

        # Проверяем успешность запроса
        if response.status_code != 200:
            logging.error(f"Ошибка запроса данных: Статус {response.status_code}")
            return []

        # Получаем данные из ответа
        result = response.json()

        # Обработка информации о сроке действия из символа
        for item in result:
            symbol = item.get('symbol')
            # Попробуем извлечь срок действия из символа, если он закодирован в символе
            expiry_info = symbol.split('-')
            if len(expiry_info) > 1:
                expiry_date = expiry_info[1]
                # Предположим, что дата истечения в формате ГГММДД (YYMMDD)
                item['expiryDate'] = f"20{expiry_date[:2]}-{expiry_date[2:4]}-{expiry_date[4:]}"
            else:
                item['expiryDate'] = 'N/A'

        logging.info(f"Всего получено {len(result)} опционных контрактов с Binance.")
        return result

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при выполнении запроса: {e}")
        return []


# Получение данных с Bybit об опционах
def get_bybit_options_data():
    # URL для получения опционных тикеров на Bybit
    url = "https://api.bybit.com/v5/market/tickers"
    base_coins = ['BTC', 'ETH']  # Список базовых активов, для которых есть опционы
    all_results = []

    for base_coin in base_coins:
        params = {
            'category': 'option',
            'baseCoin': base_coin
        }

        logging.info(f"Запрос данных об опционах {base_coin} с Bybit по адресу {url} с параметрами {params}...")

        try:
            response = requests.get(url, params=params)

            # Проверяем успешность запроса
            if response.status_code != 200:
                logging.error(f"Ошибка запроса данных: Статус {response.status_code}")
                continue

            data = response.json()

            if data.get('retCode') != 0:
                logging.error(f"Ошибка API: {data.get('retMsg')}")
                continue

            result = data.get('result', {}).get('list', [])
            all_results.extend(result)

            logging.info(f"Получено {len(result)} опционных контрактов {base_coin} с Bybit.")

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при выполнении запроса: {e}")
            continue

    logging.info(f"Всего получено {len(all_results)} опционных контрактов с Bybit.")
    return all_results


# Получение данных с OKEx об опционах
def get_okex_options_data():
    import time  # Для добавления задержки между запросами, если необходимо
    # URL для получения опционных тикеров на OKEx
    url = "https://www.okx.com/api/v5/market/tickers"
    underlyings = ['BTC-USD', 'ETH-USD']  # Список базовых активов
    all_results = []

    for uly in underlyings:
        params = {
            'instType': 'OPTION',
            'uly': uly
        }

        logging.info(f"Запрос данных об опционах {uly} с OKEx по адресу {url} с параметрами {params}...")

        try:
            response = requests.get(url, params=params)

            # Проверяем успешность запроса
            if response.status_code != 200:
                logging.error(f"Ошибка запроса данных: Статус {response.status_code}")
                continue

            data = response.json()

            if data.get('code') != '0':
                logging.error(f"Ошибка API: {data.get('msg')}")
                continue

            result = data.get('data', [])
            all_results.extend(result)

            logging.info(f"Получено {len(result)} опционных контрактов {uly} с OKEx.")

            # Если необходимо, можно добавить задержку между запросами
            # time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при выполнении запроса: {e}")
            continue

    logging.info(f"Всего получено {len(all_results)} опционных контрактов с OKEx.")
    return all_results


# Сохранение данных в базу данных
def save_to_db(data, exchange, market_type):
    conn = sqlite3.connect('opcion_data.db')
    cursor = conn.cursor()

    logging.info(f"Сохранение данных для {exchange} ({market_type}): {data[:5]}...")  # Логируем первые 5 записей

    for item in data:
        try:
            if exchange == 'Binance':
                symbol = item.get('symbol')
                strike_price = item.get('strikePrice')
                option_type = 'Call' if symbol.endswith('-C') else 'Put'
                expiry_date = item.get('expiryDate')
                exercise_price = item.get('exercisePrice', '0')

                last_price = Decimal(str(item.get('lastPrice') or '0'))
                volume_24h = Decimal(str(item.get('volume') or '0'))
                high_price_24h = Decimal(str(item.get('high') or '0'))
                low_price_24h = Decimal(str(item.get('low') or '0'))
                trades_24h = item.get('tradeCount', '0')

            elif exchange == 'Bybit':
                symbol = item.get('symbol')
                symbol_parts = symbol.split('-')
                if len(symbol_parts) != 4:
                    logging.error(f"Неизвестный формат символа: {symbol}")
                    continue
                underlying_asset, expiry_str, strike_price, option_code = symbol_parts
                option_type = 'Call' if option_code == 'C' else 'Put'

                try:
                    expiry_date = datetime.strptime(expiry_str, '%d%b%y').strftime('%Y-%m-%d')
                except ValueError as ve:
                    logging.error(f"Ошибка при разборе даты истечения для {symbol}: {ve}")
                    continue
                exercise_price = strike_price

                last_price = Decimal(str(item.get('lastPrice') or '0'))
                volume_24h = Decimal(str(item.get('turnover24h') or '0'))
                high_price_24h = Decimal(str(item.get('highPrice24h') or '0'))
                low_price_24h = Decimal(str(item.get('lowPrice24h') or '0'))
                trades_24h = '0'

            elif exchange == 'OKEx':
                symbol = item.get('instId')
                symbol_parts = symbol.split('-')
                if len(symbol_parts) != 5:
                    logging.error(f"Неизвестный формат символа: {symbol}")
                    continue
                underlying_asset, currency, expiry_str, strike_price, option_code = symbol_parts
                option_type = 'Call' if option_code == 'C' else 'Put'

                try:
                    expiry_date = datetime.strptime(expiry_str, '%y%m%d').strftime('%Y-%m-%d')
                except ValueError as ve:
                    logging.error(f"Ошибка при разборе даты истечения для {symbol}: {ve}")
                    continue

                exercise_price = strike_price

                last_price = Decimal(str(item.get('last') or '0'))
                volume_24h = Decimal(str(item.get('volCcy24h') or '0'))
                high_price_24h = Decimal(str(item.get('high24h') or '0'))
                low_price_24h = Decimal(str(item.get('low24h') or '0'))
                trades_24h = '0'

            else:
                logging.error(f"Неизвестная биржа: {exchange}")
                continue

            price_usdt = volume_24h * last_price
            last_price_str = format(last_price, 'f')
            volume_24h_str = format(volume_24h, 'f')
            price_usdt_str = format(price_usdt, 'f')
            high_price_24h_str = format(high_price_24h, 'f')
            low_price_24h_str = format(low_price_24h, 'f')
            trades_24h_str = str(trades_24h)
            updated_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # Время обновления
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                INSERT INTO opcion_data (symbol, exchange, market_type, last_price, volume_24h, options, price_usdt,
                                         high_price_24h, low_price_24h, trades_24h, strike_price, option_type, expiry_date,
                                         exercise_price, timestamp, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, exchange, market_type) DO UPDATE SET
                    last_price = excluded.last_price,
                    volume_24h = excluded.volume_24h,
                    price_usdt = excluded.price_usdt,
                    high_price_24h = excluded.high_price_24h,
                    low_price_24h = excluded.low_price_24h,
                    trades_24h = excluded.trades_24h,
                    updated_time = excluded.updated_time
            ''', (symbol, exchange, market_type, last_price_str, volume_24h_str, symbol, price_usdt_str,
                  high_price_24h_str, low_price_24h_str, trades_24h_str, strike_price, option_type, expiry_date,
                  exercise_price, timestamp, updated_time))


        except (InvalidOperation, TypeError, ValueError, KeyError) as e:
            logging.error(f"Ошибка при обработке данных: {e}")
            continue

    conn.commit()
    conn.close()
    logging.info(f"Данные успешно сохранены для {market_type} с биржи {exchange}.")


def background_update():
    logging.info("Фоновое обновление данных началось.")

    # Получаем и сохраняем данные для каждого обмена
    binance_options_data = get_binance_options_data()
    save_to_db(binance_options_data, exchange='Binance', market_type='options')

    bybit_options_data = get_bybit_options_data()
    save_to_db(bybit_options_data, exchange='Bybit', market_type='options')

    okex_options_data = get_okex_options_data()
    save_to_db(okex_options_data, exchange='OKEx', market_type='options')

    logging.info("Фоновое обновление данных завершено.")


def add_updated_time_column():
    conn = sqlite3.connect('opcion_data.db')
    cursor = conn.cursor()

    # Добавляем колонку updated_time, если её нет
    try:
        cursor.execute("ALTER TABLE opcion_data ADD COLUMN updated_time DATETIME")
        logging.info("Колонка 'updated_time' успешно добавлена.")
    except sqlite3.OperationalError:
        logging.info("Колонка 'updated_time' уже существует.")

    conn.commit()
    conn.close()


# Основной процесс: получение и сохранение данных
if __name__ == "__main__":
    create_db()  # Создаём базу данных, если её нет
    add_updated_time_column()  # Добавляем колонку updated_time, если её нет

    # Запускаем фоновое обновление данных
    update_thread = threading.Thread(target=background_update)
    update_thread.start()
    update_thread.join()  # Ожидаем завершения фонового обновления
    logging.info("Фоновое обновление данных завершено.")

    # Получаем данные об опционах с Binance
    binance_options_data = get_binance_options_data()
    # Сохраняем данные в базу данных
    save_to_db(binance_options_data, exchange='Binance', market_type='options')

    # Получаем данные об опционах с Bybit
    bybit_options_data = get_bybit_options_data()
    # Сохраняем данные в базу данных
    save_to_db(bybit_options_data, exchange='Bybit', market_type='options')

    # Получаем данные об опционах с OKEx
    okex_options_data = get_okex_options_data()
    # Сохраняем данные в базу данных
    save_to_db(okex_options_data, exchange='OKEx', market_type='options')
