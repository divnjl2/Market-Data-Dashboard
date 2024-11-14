import os
import sqlite3
import logging
import requests
import threading
import time
import json
from websocket import WebSocketApp

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bybit_websocket.log"), logging.StreamHandler()]
)

# Настройки переподключения WebSocket
RECONNECT_DELAY = 5  # Задержка перед переподключением (сек)
MAX_RETRIES = 10     # Максимум попыток переподключения
DURATION_MINUTES = 60  # Длительность работы WebSocket в минутах

# Подключение к базе данных SQLite
def create_connection():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, 'bybit_trades.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            execId TEXT PRIMARY KEY,
            symbol TEXT,
            price REAL,
            qty REAL,
            side TEXT,
            timestamp INTEGER,
            market_type TEXT  -- Спот или фьючерсы
        )
    ''')
    conn.commit()
    return conn, cursor

# Получение всех спотовых символов
def get_all_spot_symbols():
    url = "https://api.bybit.com/v5/market/tickers?category=spot"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        symbols = [ticker['symbol'] for ticker in data['result']['list']]
        return symbols
    else:
        logging.error(f"Не удалось получить спотовые символы, статус код: {response.status_code}")
        return []

# Получение всех фьючерсных символов
def get_all_futures_symbols():
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        symbols = [ticker['symbol'] for ticker in data['result']['list']]
        return symbols
    else:
        logging.error(f"Не удалось получить фьючерсные символы, статус код: {response.status_code}")
        return []

# Сохранение сделки в базу данных
def save_trade_to_db(trade, market_type):
    conn, cursor = create_connection()
    try:
        current_timestamp = int(time.time() * 1000)
        trade_timestamp = int(trade['T'])
        if current_timestamp - trade_timestamp > 4 * 60 * 60 * 1000:
            logging.info(f"Пропущена сделка вне диапазона 4 часов: {trade['i']}")
            return

        cursor.execute('''
            INSERT OR IGNORE INTO trades (execId, symbol, price, qty, side, timestamp, market_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade['i'],  # execId
            trade['s'],  # symbol
            float(trade['p']),  # price
            float(trade['v']),  # quantity
            trade['S'],  # side (Buy/Sell)
            trade_timestamp,  # timestamp
            market_type  # спот или фьючерсы
        ))
        conn.commit()
        logging.info(f"Сделка сохранена ({market_type}): {trade['i']}")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при записи в БД: {e}")
    finally:
        cursor.close()
        conn.close()

# Обработка сообщений WebSocket
def on_message(ws, message, market_type, start_time):
    if time.time() - start_time > DURATION_MINUTES * 60:
        logging.info("Время записи истекло, закрытие соединения.")
        ws.close()
        return

    try:
        data = json.loads(message)
        logging.info(f"Получено сообщение: {data}")
        if 'topic' in data and data.get('topic').startswith('publicTrade'):
            for trade in data['data']:
                save_trade_to_db(trade, market_type)
    except json.JSONDecodeError as e:
        logging.error(f"Ошибка JSON: {e}")

# Обработка ошибок WebSocket
def on_error(ws, error):
    logging.error(f"WebSocket ошибка: {error}")

# Обработка закрытия WebSocket
def on_close(ws, close_status_code, close_msg):
    logging.info(f"Соединение закрыто: {close_status_code}, {close_msg}")

# Переподключение WebSocket
def ws_reconnect(ws):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            logging.info(f"Попытка переподключения {attempt + 1}/{MAX_RETRIES}")
            ws.run_forever()
            return
        except Exception as e:
            logging.error(f"Ошибка при переподключении: {e}")
            attempt += 1
            time.sleep(RECONNECT_DELAY)
    logging.error("Максимум попыток переподключения исчерпан, остановка WebSocket.")

# Обработка успешного открытия WebSocket соединения
def on_open(ws, symbols, market_type):
    logging.info(f"Соединение открыто для {market_type}")
    for symbol in symbols:
        ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{symbol}"]}))
        logging.info(f"Подписка на пару {symbol} ({market_type})")

# Основная функция для запуска WebSocket
def run_websocket(duration_minutes=DURATION_MINUTES):
    start_time = time.time()  # Время начала
    spot_symbols = get_all_spot_symbols()
    futures_symbols = get_all_futures_symbols()

    if spot_symbols:
        ws_spot = WebSocketApp(
            "wss://stream.bybit.com/v5/public/spot",
            on_message=lambda ws, msg: on_message(ws, msg, 'spot', start_time),
            on_error=on_error,
            on_close=on_close
        )
        ws_spot.on_open = lambda ws: on_open(ws, spot_symbols, 'spot')
        threading.Thread(target=ws_spot.run_forever).start()

    if futures_symbols:
        ws_futures = WebSocketApp(
            "wss://stream.bybit.com/v5/public/linear",
            on_message=lambda ws, msg: on_message(ws, msg, 'futures', start_time),
            on_error=on_error,
            on_close=on_close
        )
        ws_futures.on_open = lambda ws: on_open(ws, futures_symbols, 'futures')
        threading.Thread(target=ws_futures.run_forever).start()

    # Ожидание завершения по времени, без повторного закрытия в stop_websocket
    while time.time() - start_time < duration_minutes * 60:
        time.sleep(1)
    stop_websocket(ws_spot, ws_futures)

# Завершение работы WebSocket
def stop_websocket(ws_spot, ws_futures):
    logging.info("Закрытие WebSocket после завершения времени работы")
    ws_spot.close()
    ws_futures.close()

# Запуск программы
if __name__ == "__main__":
    run_websocket()
