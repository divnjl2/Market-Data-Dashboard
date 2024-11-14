from dash import dash_table, dcc, html, Input, Output, callback_context
import pandas as pd
import sqlite3
import os
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, callback_context
import subprocess
import sys
import time
import threading



DATABASE_CONFIG = {
    "Binance": {
        "spot": {
            "main": {"db_path": "market_data.db", "table": "market_data"}
        },
        "futures": {
            "main": {"db_path": "market_data.db", "table": "market_data"}
        },
        "options": {
            "main": {"db_path": "opcion_data.db", "table": "opcion_data"}
        }
    },
    "Bybit": {
        "spot": {
            "main": {
                "db_path": "market_data.db",
                "table": "market_data",
                "columns": ["symbol", "exchange", "market_type", "last_price", "volume_24h", "price_usdt", "high_price_24h", "low_price_24h", "trades_24h", "timestamp"]
            },
            "trades": {
                "db_path": "bybit_analysis.db",
                "table": "trade_analysis"
            }
        },
        "futures": {
            "main": {
                "db_path": "market_data.db",
                "table": "market_data",
                "columns": ["symbol", "exchange", "market_type", "last_price", "volume_24h", "price_usdt", "high_price_24h", "low_price_24h", "trades_24h", "timestamp"]
            },
            "trades": {
                "db_path": "trade_analysis.db",
                "table": "trade_analysis"
            }
        },
        "options": {
            "main": {"db_path": "opcion_data.db", "table": "opcion_data"}
        }
    },
    "OKX": {
        "spot": {
            "main": {
                "db_path": "market_data.db",
                "table": "market_data"
            },
            "trades": {
                "db_path": "trades_data_okx.db",
                "table": "trades_data"
            }
        },
        "futures": {
            "main": {
                "db_path": "futures_data.db",
                "table": "futures_data"
            },
            "trades": {
                "db_path": "trades_data_okx.db",
                "table": "trades_data"
            },
            "column_mapping": {  # Указываем, какие столбцы использовать для Dash
                "volume_24h": "volume_24h_base_currency",
                "price_usdt": "turnover_24h_usd",
                "high_price_24h": "high_price",  # Например, используем last_price временно
                "low_price_24h": "low_price"   # Используем last_price временно
            }
        },
        "options": {
            "main": {
                "db_path": "opcion_data.db",
                "table": "opcion_data"
            }
        }
    }
}


# Унифицированный формат столбцов для всех типов рынков
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

def standardize_columns(df, exchange, market_type):
    required_columns = COLUMN_CONFIG.get(market_type, [])

    df['exchange'] = exchange
    df['market_type'] = market_type

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    return df[required_columns]

# Обновляем функцию fetch_data_from_db, добавляя условие для OKX
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

    # Подключаемся к основной базе данных и получаем данные
    conn_main = sqlite3.connect(db_path_main)
    try:
        # Добавляем условие фильтрации по market_type
        query = f"SELECT * FROM {table_main} WHERE market_type = '{market_type}'"
        main_df = pd.read_sql_query(query, conn_main)
    except sqlite3.OperationalError:
        print(f"Таблица {table_main} не найдена в базе данных {db_path_main}.")
        return pd.DataFrame(columns=COLUMN_CONFIG.get(market_type, UNIFIED_COLUMNS))
    finally:
        conn_main.close()

    # Применяем маппинг столбцов, если он указан для биржи и типа рынка
    column_mapping = config.get("column_mapping", {})
    for target_col, source_col in column_mapping.items():
        if source_col in main_df.columns:
            main_df[target_col] = main_df[source_col]
        else:
            main_df[target_col] = None

    # Обработка данных для трейдов в зависимости от биржи и типа рынка
    if exchange == "Bybit":
        main_df = fetch_bybit_trades_data(main_df, exchange)
    elif exchange == "OKX":
        main_df = fetch_okx_trades_data(main_df, exchange)

    # Устранение несовпадения столбцов и стандартизация
    main_df = standardize_columns(main_df, exchange, market_type)
    return main_df


def fetch_bybit_trades_data(df, exchange):
    # Конфигурация для трейдов Bybit
    config_trades = DATABASE_CONFIG.get(exchange, {}).get("spot", {}).get("trades", None)
    if not config_trades:
        print("Конфигурация трейдов Bybit не найдена.")
        return df

    db_path_trades = config_trades["db_path"]
    table_trades = config_trades["table"]

    # Проверяем существование базы данных
    if not os.path.exists(db_path_trades):
        print(f"База данных {db_path_trades} не существует.")
        return df

    # Подключаемся и проверяем наличие данных
    conn_trades = sqlite3.connect(db_path_trades)
    try:
        conn_trades.execute(f"SELECT 1 FROM {table_trades} LIMIT 1;")
        trades_df = pd.read_sql_query(
            f"SELECT symbol, total_trades FROM {table_trades} WHERE exchange = '{exchange}'", conn_trades)
        # Добавляем трейды, соединяя по `symbol`
        df = df.merge(trades_df[['symbol', 'total_trades']], on='symbol', how='left')
        df.rename(columns={"total_trades": "trades_24h"}, inplace=True)
    except sqlite3.OperationalError:
        print(f"Таблица {table_trades} не найдена в базе данных {db_path_trades}.")
    finally:
        conn_trades.close()

    return df

# Обновляем функцию fetch_data_from_db, добавляя условие для Bybit
def fetch_okx_trades_data(df, exchange):
    # Конфигурация для трейдов OKX
    config_trades = DATABASE_CONFIG.get(exchange, {}).get("spot", {}).get("trades", None)
    if not config_trades:
        print("Конфигурация трейдов OKX не найдена.")
        return df

    db_path_trades = config_trades["db_path"]
    table_trades = config_trades["table"]

    # Проверяем существование базы данных
    if not os.path.exists(db_path_trades):
        print(f"База данных {db_path_trades} не существует.")
        return df

    # Подключаемся и проверяем наличие данных
    conn_trades = sqlite3.connect(db_path_trades)
    try:
        conn_trades.execute(f"SELECT 1 FROM {table_trades} LIMIT 1;")
        trades_df = pd.read_sql_query(
            f"SELECT symbol, total_trades FROM {table_trades} WHERE exchange = '{exchange}'", conn_trades)
        # Добавляем трейды, соединяя по `symbol`
        df = df.merge(trades_df[['symbol', 'total_trades']], on='symbol', how='left')
        df.rename(columns={"total_trades": "trades_24h"}, inplace=True)
    except sqlite3.OperationalError:
        print(f"Таблица {table_trades} не найдена в базе данных {db_path_trades}.")
    finally:
        conn_trades.close()

    return df

# Инициализация приложения Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG], suppress_callback_exceptions=True)

# Главная страница
def main_page_layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col(html.H1("Market Data", id='header', style={'textAlign': 'center'}), width=12)
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='exchange-filter',
                    options=[
                        {'label': 'Binance', 'value': 'Binance'},
                        {'label': 'Bybit', 'value': 'Bybit'},
                        {'label': 'OKX', 'value': 'OKX'}
                    ],
                    placeholder='Выберите биржу',
                    style={'width': '100%', 'margin-bottom': '10px'}
                ),
                width=4
            ),
            dbc.Col(
                dcc.Dropdown(
                    id='market-type-filter',
                    options=[
                        {'label': 'Spot', 'value': 'spot'},
                        {'label': 'Futures', 'value': 'futures'},
                        {'label': 'Options', 'value': 'options'}
                    ],
                    placeholder='Выберите тип рынка',
                    style={'width': '100%', 'margin-bottom': '10px'}
                ),
                width=4
            ),
        ]),
        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id='market_data_table',
                    columns=[{"name": col.capitalize().replace('_', ' '), "id": col} for col in COLUMN_CONFIG["spot"]],
                    data=[],
                    sort_action="native",
                    sort_mode="multi",
                    filter_action="native",
                    page_size=20,
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'center',
                        'padding': '5px',
                        'whiteSpace': 'normal',
                        'height': 'auto',
                        'backgroundColor': '#1e1e1e',
                        'color': '#FFFFFF'
                    },
                    style_header={
                        'fontWeight': 'bold',
                        'backgroundColor': '#1e1e1e',
                        'color': '#FFFFFF'
                    }
                ),
                width=12
            )
        ])
    ], fluid=True, id='main-container')



# Хранилище для логов
logs = []

# Функция для запуска процесса и считывания логов в отдельном потоке
def run_script(script_path):
    try:
        # Запуск скрипта с параметром `-u` для отключения буферизации вывода
        process = subprocess.Popen(
            [sys.executable, "-u", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # Установка небуферизованного режима
        )

        # Чтение вывода построчно в реальном времени
        for line in process.stdout:
            logs.append(line)
            print(f"STDOUT: {line}")  # Отладочный вывод в стандартный поток

        for line in process.stderr:
            logs.append(line)
            print(f"STDERR: {line}")  # Отладочный вывод в стандартный поток

        # По завершении процесса выводим сообщение
        process.wait()
        logs.append(f"{script_path} завершён.\n")
    except Exception as e:
        logs.append(f"Ошибка при запуске {script_path}: {str(e)}\n")

# Страница управления процессами с кнопками для каждой задачи
def process_management_page():
    return dbc.Container([
        dbc.Row([
            dbc.Col(html.H2("Управление процессами"), width=12)
        ]),
        dbc.Row([
            dbc.Col(dbc.Button("Обновить основные данные", id="update-main-btn", color="primary"), width="auto"),
            dbc.Col(dbc.Button("Обновить опционы", id="update-options-btn", color="info"), width="auto"),
            dbc.Col(dbc.Button("Обновить фьючерсы OKX", id="update-futures-okx-btn", color="warning"), width="auto"),
            dbc.Col(dbc.Button("Обновить трейды OKX", id="update-trades-okx-btn", color="danger"), width="auto"),
            dbc.Col(dbc.Button("Обновить трейды Bybit", id="update-trades-bybit-btn", color="success"), width="auto")
        ]),
        dbc.Row([
            dbc.Col(html.H3("Консоль логов выполнения"), width=12)
        ]),
        dbc.Row([
            dbc.Col(dcc.Textarea(id="console-output", style={"width": "100%", "height": "300px", "backgroundColor": "#1e1e1e", "color": "#00FF00"}), width=12)
        ]),
        # Интервал для обновления консоли
        dcc.Interval(id="console-update-interval", interval=2000, n_intervals=0)
    ], fluid=True)

# Layout с меню навигации и загрузкой нужной страницы
app.layout = dbc.Container([
    dcc.Location(id="url"),
    dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Главная", href="/")),
            dbc.NavItem(dbc.NavLink("Управление процессами", href="/process-management"))
        ],
        brand="Приложение Dash",
        color="light",
        dark=False
    ),
    html.Div(id="page-content")
], fluid=True)

# Колбэк для переключения страниц
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname):
    if pathname == "/process-management":
        return process_management_page()
    else:
        return main_page_layout()

# Колбэк для запуска скриптов в отдельных потоках
@app.callback(
    Output("console-output", "value"),
    [Input("update-main-btn", "n_clicks"),
     Input("update-options-btn", "n_clicks"),
     Input("update-futures-okx-btn", "n_clicks"),
     Input("update-trades-okx-btn", "n_clicks"),
     Input("update-trades-bybit-btn", "n_clicks"),
     Input("console-update-interval", "n_intervals")],
    prevent_initial_call=True
)
def run_scripts(n_main, n_options, n_futures_okx, n_trades_okx, n_trades_bybit, n_intervals):
    # Определяем, какая кнопка была нажата
    triggered_id = callback_context.triggered[0]["prop_id"].split(".")[0]

    # Определение скрипта по нажатию кнопки
    script_map = {
        "update-main-btn": "main.py",
        "update-options-btn": "options.py",
        "update-futures-okx-btn": "update_futures_okx.py",
        "update-trades-okx-btn": "update_trades_okx.py",
        "update-trades-bybit-btn": "update_trades_bybit.py"
    }


# Ваши существующие колбэки, например, для обновления таблицы
@app.callback(
    [Output('market_data_table', 'data'),
     Output('market_data_table', 'columns')],
    [Input('exchange-filter', 'value'),
     Input('market-type-filter', 'value')]
)
def update_table(exchange, market_type):
    if not exchange or not market_type:
        return [], [{"name": col.capitalize().replace('_', ' '), "id": col} for col in UNIFIED_COLUMNS]

    df = fetch_data_from_db(exchange, market_type)
    if df.empty:
        return [], [{"name": col.capitalize().replace('_', ' '), "id": col} for col in
                    COLUMN_CONFIG.get(market_type, UNIFIED_COLUMNS)]

    selected_columns = COLUMN_CONFIG.get(market_type, UNIFIED_COLUMNS)
    columns = [{"name": col.capitalize().replace('_', ' '), "id": col} for col in selected_columns]

    return df.to_dict('records'), columns


if __name__ == "__main__":
    app.run_server(debug=True)