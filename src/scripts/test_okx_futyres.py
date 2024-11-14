import sqlite3
import logging
import requests
import logging
from datetime import datetime, timezone

# Настройка логирования
logging.basicConfig(level=logging.INFO)


def create_db():
    conn = sqlite3.connect('futures_data.db')
    cursor = conn.cursor()

    # Удаление таблицы, если она существует
    cursor.execute('DROP TABLE IF EXISTS futures_data')
    logging.info("Таблица futures_data удалена.")

    # Создание таблицы с обновлённой структурой
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
    logging.info("Таблица market_data создана заново.")

    conn.commit()
    conn.close()

def save_to_db(data, exchange, market_type):
    conn = sqlite3.connect('okx_futures.db')
    cursor = conn.cursor()

    logging.info(f"Сохранение данных для {exchange} ({market_type})")

    for item in data:
        try:
            symbol = item.get('symbol')
            last_price = float(item.get('last_price') or 0)
            low_price = float(item.get('low_price') or 0)
            high_price = float(item.get('high_price') or 0)
            # (другие поля остаются без изменений)

            cursor.execute("SELECT id FROM market_data WHERE symbol = ?", (symbol,))
            existing_record = cursor.fetchone()

            if existing_record:
                # Обновление записи
                cursor.execute('''
                    UPDATE market_data
                    SET last_price = ?, low_price = ?, high_price = ?, volume_24h_contracts = ?, 
                        volume_24h_base_currency = ?, turnover_24h_usd = ?, open_interest_contracts = ?, 
                        open_interest_base_currency = ?, contract_size = ?, contract_type = ?, timestamp = ?
                    WHERE symbol = ?
                ''', (last_price, low_price, high_price, ...))
            else:
                # Вставка новой записи
                cursor.execute('''
                    INSERT INTO market_data (symbol, exchange, market_type, last_price, low_price, high_price, ...)
                    VALUES (?, ?, ?, ?, ?, ?, ...)
                ''', (symbol, exchange, market_type, last_price, low_price, high_price, ...))

        except Exception as e:
            logging.error(f"Ошибка при обработке данных для {symbol}: {e}", exc_info=True)

    conn.commit()
    conn.close()


def fetch_contract_data(inst_id):
    url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('data', [])
        if not data:
            logging.error(f"Данные для контракта {inst_id} не получены.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе данных для контракта {inst_id}: {e}")
        return []



# Настройка логирования
logging.basicConfig(level=logging.INFO)


def process_contract_data(contract_data):
    """
    Получает и логирует данные по всем SWAP контрактам.
    """
    url_instruments = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"

    try:
        # Получаем список всех SWAP инструментов
        response_instruments = requests.get(url_instruments)
        response_instruments.raise_for_status()
        data_instruments = response_instruments.json()

        if not data_instruments.get('data'):
            logging.error("Не удалось получить список SWAP инструментов.")
            return

        instruments = data_instruments['data']
        processed_data = []

        # Собираем данные для каждого инструмента
        for instrument in instruments:
            inst_id = instrument['instId']
            contract_size = float(instrument.get('ctVal', 0))
            ctValCcy = instrument.get('ctValCcy', '')

            if contract_size == 0:
                logging.warning(f"Размер контракта для {inst_id} равен нулю, пропускаем.")
                continue

            # Получаем данные по тикеру
            url_ticker = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
            try:
                response_ticker = requests.get(url_ticker)
                response_ticker.raise_for_status()
                data_ticker = response_ticker.json()

                if not data_ticker.get('data'):
                    logging.error(f"Данные для контракта {inst_id} не получены.")
                    continue

                # Извлекаем необходимые значения из тикера
                item = data_ticker['data'][0]
                last_price = float(item.get('last', 0))
                low_price = float(item.get('low24h', 0))  # Низкая цена за 24 часа
                high_price = float(item.get('high24h', 0))  # Высокая цена за 24 часа
                volume_24h_contracts = float(item.get('vol24h', 0))
                volCcy24h = float(item.get('volCcy24h', 0))

                # Объём за 24 часа в базовой валюте
                volume_24h_base_currency = volume_24h_contracts * contract_size

                # Оборот за 24 часа в USD
                volume_24h_usd = volume_24h_base_currency * last_price

                # Получаем открытый интерес
                url_open_interest = f"https://www.okx.com/api/v5/public/open-interest?instId={inst_id}"
                try:
                    response_oi = requests.get(url_open_interest)
                    response_oi.raise_for_status()
                    data_oi = response_oi.json()

                    if not data_oi.get('data'):
                        logging.error(f"Данные об открытом интересе для {inst_id} не получены.")
                        continue

                    open_interest_contracts = float(data_oi['data'][0]['oi'])
                    open_interest_base_currency = open_interest_contracts * contract_size

                    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(
                        f"{inst_id}: Последняя цена: {last_price}, Низкая цена за 24ч: {low_price}, "
                        f"Высокая цена за 24ч: {high_price}, Объём за 24ч в контрактах: {volume_24h_contracts}, "
                        f"Объём за 24ч в {ctValCcy}: {volume_24h_base_currency}, Оборот за 24ч в USD: {volume_24h_usd}, "
                        f"Открытый интерес: {open_interest_contracts} контрактов "
                        f"({open_interest_base_currency} {ctValCcy}), Размер контракта: {contract_size} {ctValCcy}, "
                        f"Время: {timestamp}"
                    )

                    # Добавляем данные в список для сохранения в БД
                    processed_data.append({
                        'symbol': inst_id,
                        'last_price': last_price,
                        'low_price': low_price,
                        'high_price': high_price,
                        'volume_24h_contracts': volume_24h_contracts,
                        'volume_24h_base_currency': volume_24h_base_currency,
                        'turnover_24h_usd': volume_24h_usd,
                        'open_interest_contracts': open_interest_contracts,
                        'open_interest_base_currency': open_interest_base_currency,
                        'contract_size': contract_size,
                        'contract_type': ctValCcy,
                        'timestamp': timestamp
                    })

                except requests.exceptions.RequestException as e:
                    logging.error(f"Ошибка при запросе открытого интереса для {inst_id}: {e}")
                    continue

            except requests.exceptions.RequestException as e:
                logging.error(f"Ошибка при запросе данных для контракта {inst_id}: {e}")
                continue

        return processed_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при выполнении запросов: {e}")
        return []


def get_okx_futures_data():
    categories = ['SWAP', 'FUTURES']
    data = []

    for category in categories:
        url = f"https://www.okx.com/api/v5/public/instruments?instType={category}"
        logging.info(f"Запрос данных с OKX (фьючерсы) ({category})...")

        response = requests.get(url)
        response.raise_for_status()
        result = response.json().get('data', [])

        for item in result:
            symbol = item.get('instId')

            contract_data = fetch_contract_data(symbol)
            if contract_data:
                processed_contract_data = process_contract_data(contract_data, )
                if processed_contract_data:  # Проверяем, что данные не пустые
                    data.extend(processed_contract_data)

    logging.info(f"Всего получено {len(data)} обработанных контрактов с OKX (фьючерсы).")
    return data


if __name__ == "__main__":
    create_db()


    futures_data = get_okx_futures_data()
    save_to_db(futures_data, exchange='OKX', market_type='futures')
