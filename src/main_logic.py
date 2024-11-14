# main_logic.py
import pandas as pd
import sqlite3
import os

# main_logic.py (или файл, где вы объявляете DATABASE_CONFIG)
import os

# Задаем базовый путь к директории `databases`
DATABASE_BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../databases"))

# Конфигурация баз данных с использованием абсолютных путей
DATABASE_CONFIG = {
    "Binance": {
        "spot": {
            "main": {"db_path": os.path.join(DATABASE_BASE_PATH, "market_data.db"), "table": "market_data"}
        },
        "futures": {
            "main": {"db_path": os.path.join(DATABASE_BASE_PATH, "market_data.db"), "table": "market_data"}
        },
        "options": {
            "main": {"db_path": os.path.join(DATABASE_BASE_PATH, "opcion_data.db"), "table": "opcion_data"}
        }
    },
    "Bybit": {
        "spot": {
            "main": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "market_data.db"),
                "table": "market_data",
                "columns": ["symbol", "exchange", "market_type", "last_price", "volume_24h", "price_usdt", "high_price_24h", "low_price_24h", "trades_24h", "timestamp"]
            },
            "trades": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "bybit_analysis.db"),
                "table": "trade_analysis"
            }
        },
        "futures": {
            "main": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "market_data.db"),
                "table": "market_data",
                "columns": ["symbol", "exchange", "market_type", "last_price", "volume_24h", "price_usdt", "high_price_24h", "low_price_24h", "trades_24h", "timestamp"]
            },
            "trades": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "trade_analysis.db"),
                "table": "trade_analysis"
            }
        },
        "options": {
            "main": {"db_path": os.path.join(DATABASE_BASE_PATH, "opcion_data.db"), "table": "opcion_data"}
        }
    },
    "OKX": {
        "spot": {
            "main": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "market_data.db"),
                "table": "market_data"
            },
            "trades": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "trades_data_okx.db"),
                "table": "trades_data"
            }
        },
        "futures": {
            "main": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "futures_data.db"),
                "table": "futures_data"
            },
            "trades": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "trades_data_okx.db"),
                "table": "trades_data"
            },
            "column_mapping": {
                "volume_24h": "volume_24h_base_currency",
                "price_usdt": "turnover_24h_usd",
                "high_price_24h": "high_price",
                "low_price_24h": "low_price"
            }
        },
        "options": {
            "main": {
                "db_path": os.path.join(DATABASE_BASE_PATH, "opcion_data.db"),
                "table": "opcion_data"
            }
        }
    }
}


# Унифицированный формат столбцов
UNIFIED_COLUMNS = [
    "symbol", "exchange", "market_type", "last_price", "volume_24h", "price_usdt",
    "high_price_24h", "low_price_24h", "trades_24h", "timestamp",
    "strike_price", "option_type", "expiry_date", "exercise_price"
]

# Определение столбцов для каждого типа рынка
COLUMN_CONFIG = {
    "spot": [
        "symbol", "exchange", "market_type", "last_price", "volume_24h",
        "price_usdt", "high_price_24h", "low_price_24h", "trades_24h", "timestamp"
    ],
    "futures": [
        "symbol", "exchange", "market_type", "last_price", "volume_24h",
        "price_usdt", "high_price_24h", "low_price_24h", "trades_24h", "timestamp"
    ],
    "options": [
        "symbol", "exchange", "market_type", "last_price", "volume_24h", "price_usdt",
        "high_price_24h", "low_price_24h", "trades_24h", "timestamp",
        "strike_price", "option_type", "expiry_date", "exercise_price"
    ]
}

# Функция для стандартизации столбцов
def standardize_columns(df, exchange, market_type):
    required_columns = COLUMN_CONFIG.get(market_type, [])

    df['exchange'] = exchange
    df['market_type'] = market_type

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    return df[required_columns]

# Функция для извлечения данных из базы данных
def fetch_data_from_db(exchange=None, market_type=None):
    if not exchange or not market_type:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    config = DATABASE_CONFIG.get(exchange, {}).get(market_type, None)
    if not config:
        print(f"Конфигурация не найдена для {exchange} {market_type}")
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    db_path_main = config["main"]["db_path"]
    table_main = config["main"]["table"]

    if not os.path.exists(db_path_main):
        print(f"База данных {db_path_main} не существует.")
        return pd.DataFrame(columns=COLUMN_CONFIG.get(market_type, UNIFIED_COLUMNS))

    conn_main = sqlite3.connect(db_path_main)
    try:
        query = f"SELECT * FROM {table_main} WHERE market_type = '{market_type}'"
        main_df = pd.read_sql_query(query, conn_main)
    except sqlite3.OperationalError:
        print(f"Таблица {table_main} не найдена в базе данных {db_path_main}.")
        return pd.DataFrame(columns=COLUMN_CONFIG.get(market_type, UNIFIED_COLUMNS))
    finally:
        conn_main.close()

    column_mapping = config.get("column_mapping", {})
    for target_col, source_col in column_mapping.items():
        if source_col in main_df.columns:
            main_df[target_col] = main_df[source_col]
        else:
            main_df[target_col] = None

    if exchange == "Bybit":
        main_df = fetch_bybit_trades_data(main_df, exchange)
    elif exchange == "OKX":
        main_df = fetch_okx_trades_data(main_df, exchange)

    main_df = standardize_columns(main_df, exchange, market_type)
    return main_df

# Функция для извлечения данных о трейдах с Bybit
def fetch_bybit_trades_data(df, exchange):
    config_trades = DATABASE_CONFIG.get(exchange, {}).get("spot", {}).get("trades", None)
    if not config_trades:
        print("Конфигурация трейдов Bybit не найдена.")
        return df

    db_path_trades = config_trades["db_path"]
    table_trades = config_trades["table"]

    if not os.path.exists(db_path_trades):
        print(f"База данных {db_path_trades} не существует.")
        return df

    conn_trades = sqlite3.connect(db_path_trades)
    try:
        trades_df = pd.read_sql_query(
            f"SELECT symbol, total_trades FROM {table_trades} WHERE exchange = '{exchange}'", conn_trades)
        df = df.merge(trades_df[['symbol', 'total_trades']], on='symbol', how='left')
        df.rename(columns={"total_trades": "trades_24h"}, inplace=True)
    except sqlite3.OperationalError:
        print(f"Таблица {table_trades} не найдена в базе данных {db_path_trades}.")
    finally:
        conn_trades.close()

    return df

# Функция для извлечения данных о трейдах с OKX
def fetch_okx_trades_data(df, exchange):
    config_trades = DATABASE_CONFIG.get(exchange, {}).get("spot", {}).get("trades", None)
    if not config_trades:
        print("Конфигурация трейдов OKX не найдена.")
        return df

    db_path_trades = config_trades["db_path"]
    table_trades = config_trades["table"]

    if not os.path.exists(db_path_trades):
        print(f"База данных {db_path_trades} не существует.")
        return df

    conn_trades = sqlite3.connect(db_path_trades)
    try:
        trades_df = pd.read_sql_query(
            f"SELECT symbol, total_trades FROM {table_trades} WHERE exchange = '{exchange}'", conn_trades)
        df = df.merge(trades_df[['symbol', 'total_trades']], on='symbol', how='left')
        df.rename(columns={"total_trades": "trades_24h"}, inplace=True)
    except sqlite3.OperationalError:
        print(f"Таблица {table_trades} не найдена в базе данных {db_path_trades}.")
    finally:
        conn_trades.close()

    return df
