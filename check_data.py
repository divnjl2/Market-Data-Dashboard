import dash
import dash_bootstrap_components as dbc
from dash import dash_table, dcc, html, Input, Output, State
import plotly.graph_objs as go
import pandas as pd
import sqlite3
import requests
from datetime import datetime, timedelta

# Функция для получения данных из базы данных
def fetch_data_from_db():
    conn = sqlite3.connect('market_data.db')
    query = "SELECT * FROM market_data"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Функции для получения данных с различных бирж (Binance, Bybit, OKX)
def get_data_binance(symbol):
    url = 'https://api.binance.com/api/v3/klines'
    params = {
        'symbol': symbol,
        'interval': '1m',  # Интервал 1 минута
        'limit': 120  # Получаем данные за последние 2 часа
    }
    response = requests.get(url, params=params)
    data = response.json()
    if len(data) == 0:
        return pd.DataFrame()  # Возвращаем пустой DataFrame, если данные отсутствуют
    df = pd.DataFrame(data, columns=[
        'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close time', 'Quote asset volume', 'Number of trades',
        'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
    ])
    df['Open time'] = pd.to_datetime(df['Open time'], unit='ms') + timedelta(hours=3)  # Поправка на MSK
    df['Open'] = df['Open'].astype(float)
    df['High'] = df['High'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['Close'] = df['Close'].astype(float)
    return df

def get_data_bybit(symbol):
    url = f'https://api.bybit.com/v2/public/kline/list'
    params = {
        'symbol': symbol,
        'interval': 1,
        'limit': 120
    }
    response = requests.get(url, params=params)
    data = response.json()
    if data['ret_code'] != 0:
        return pd.DataFrame()  # Возвращаем пустой DataFrame, если данные отсутствуют
    df = pd.DataFrame(data['result'])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='s') + timedelta(hours=3)
    df.rename(columns={'open_time': 'Open time', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
    df['Open'] = df['Open'].astype(float)
    df['High'] = df['High'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['Close'] = df['Close'].astype(float)
    return df

def get_data_okx(symbol):
    url = f'https://www.okx.com/api/v5/market/candles'
    params = {
        'instId': symbol,
        'bar': '1m',
        'limit': 120
    }
    response = requests.get(url, params=params)
    data = response.json()
    if data['code'] != '0':
        return pd.DataFrame()  # Возвращаем пустой DataFrame, если данные отсутствуют
    df = pd.DataFrame(data['data'], columns=['ts', 'o', 'h', 'l', 'c', 'volume'])
    df['Open time'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=3)
    df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close'}, inplace=True)
    df['Open'] = df['Open'].astype(float)
    df['High'] = df['High'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['Close'] = df['Close'].astype(float)
    return df

# Функции для получения ордербуков с различных бирж (Binance, Bybit, OKX)
def get_order_book_binance(symbol):
    url = 'https://api.binance.com/api/v3/depth'
    params = {
        'symbol': symbol,
        'limit': 20  # Лимит до 20 уровней на каждую сторону
    }
    response = requests.get(url, params=params)
    data = response.json()
    if 'bids' in data and 'asks' in data:
        bids = pd.DataFrame(data['bids'], columns=['Price', 'Quantity'], dtype=float)
        asks = pd.DataFrame(data['asks'], columns=['Price', 'Quantity'], dtype=float)
        return bids, asks
    else:
        return pd.DataFrame(columns=['Price', 'Quantity']), pd.DataFrame(columns=['Price', 'Quantity'])

def get_order_book_bybit(symbol):
    url = f'https://api.bybit.com/v2/public/orderBook/L2'
    params = {'symbol': symbol}
    response = requests.get(url, params=params)
    data = response.json()
    if data['ret_code'] != 0:
        return pd.DataFrame(columns=['Price', 'Quantity']), pd.DataFrame(columns=['Price', 'Quantity'])
    bids = pd.DataFrame([x for x in data['result'] if x['side'] == 'Buy'], columns=['price', 'size'], dtype=float)
    asks = pd.DataFrame([x for x in data['result'] if x['side'] == 'Sell'], columns=['price', 'size'], dtype=float)
    bids.rename(columns={'price': 'Price', 'size': 'Quantity'}, inplace=True)
    asks.rename(columns={'price': 'Price', 'size': 'Quantity'}, inplace=True)
    return bids, asks

def get_order_book_okx(symbol):
    url = f'https://www.okx.com/api/v5/market/books'
    params = {
        'instId': symbol,
        'sz': 20
    }
    response = requests.get(url, params=params)
    data = response.json()
    if data['code'] != '0':
        return pd.DataFrame(columns=['Price', 'Quantity']), pd.DataFrame(columns=['Price', 'Quantity'])
    bids = pd.DataFrame(data['data'][0]['bids'], columns=['Price', 'Quantity'], dtype=float)
    asks = pd.DataFrame(data['data'][0]['asks'], columns=['Price', 'Quantity'], dtype=float)
    return bids, asks

# Инициализация приложения Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
df = fetch_data_from_db()

# Layout приложения
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(
            dcc.RadioItems(
                id='theme-switch',
                options=[
                    {'label': 'Светлая тема', 'value': 'light'},
                    {'label': 'Тёмная тема', 'value': 'dark'}
                ],
                value='dark',
                labelStyle={'display': 'inline-block', 'margin-right': '10px'}
            ),
            width='auto'
        ),
    ], justify='end', style={'padding': '10px'}),
    dbc.Row([
        dbc.Col(
            html.H1("Market Data Dashboard", id='header', style={'textAlign': 'center'}),
            width=12
        )
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
        dbc.Col(
            dcc.Input(
                id='price-filter',
                type='number',
                placeholder='Фильтр по цене...',
                style={'width': '100%', 'margin-bottom': '10px'}
            ),
            width=2
        ),
        dbc.Col(
            dcc.Input(
                id='volume-filter',
                type='number',
                placeholder='Фильтр по объему...',
                style={'width': '100%', 'margin-bottom': '10px'}
            ),
            width=2
        )
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Input(
                id='search-input',
                type='text',
                placeholder='Введите символ актива (например, BTCUSDT)...',
                style={'width': '100%', 'margin-bottom': '10px'}
            ),
            width=4
        ),
    ]),
    dbc.Row([
        dbc.Col(
            html.Div(id='order-book-div', style={'height': '600px', 'overflowY': 'scroll', 'backgroundColor': '#1e1e1e', 'color': '#FFFFFF', 'padding': '10px', 'border': '1px solid #444444'}),
            width=4
        ),
        dbc.Col(
            dcc.Graph(id='candlestick-chart', style={'height': '600px'}),
            width=8
        )
    ]),
    dbc.Row([
        dbc.Col(
            dash_table.DataTable(
                id='market_data_table',
                columns=[
                    {"name": "Symbol", "id": "symbol"},
                    {"name": "Exchange", "id": "exchange"},
                    {"name": "Market Type", "id": "market_type"},
                    {"name": "Strike Price", "id": "strike_price"},
                    {"name": "Expiry Date", "id": "expiry_date"},
                    {"name": "Last Price", "id": "last_price", "type": "numeric", "format": {"specifier": ".8f"}},
                    {"name": "Volume 24h", "id": "volume_24h", "type": "numeric", "format": {"specifier": ".2f"}},
                    {"name": "Price Usdt", "id": "price_usdt", "type": "numeric", "format": {"specifier": ".8f"}},
                    {"name": "High Price 24h", "id": "high_price_24h", "type": "numeric", "format": {"specifier": ".8f"}},
                    {"name": "Low Price 24h", "id": "low_price_24h", "type": "numeric", "format": {"specifier": ".8f"}},
                    {"name": "Trades 24h", "id": "trades_24h", "type": "numeric", "format": {"specifier": ".0f"}},
                    {"name": "Timestamp", "id": "timestamp"}
                ],
                data=df.to_dict('records'),
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
                },
                style_data_conditional=[],
            ),
            width=12
        )
    ]),
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Обновление каждые 5 секунд
        n_intervals=0
    )
], fluid=True, id='main-container')

# Коллбэк для обновления таблицы данных в зависимости от фильтров и поиска
@app.callback(
    Output('market_data_table', 'data'),
    [Input('exchange-filter', 'value'),
     Input('market-type-filter', 'value'),
     Input('price-filter', 'value'),
     Input('volume-filter', 'value'),
     Input('search-input', 'value')]
)
def update_table(exchange, market_type, price, volume, search_value):
    df = fetch_data_from_db()

    # Фильтрация по выбранной бирже
    if exchange:
        df = df[(df['exchange'].str.lower().replace({'okex': 'okx'}) == exchange.lower())]

    # Фильтрация по типу рынка
    if market_type:
        df = df[df['market_type'].str.lower() == market_type.lower()]

    # Фильтрация по цене
    if price is not None:
        df = df[df['last_price'] >= price]

    # Фильтрация по объему
    if volume is not None:
        df = df[df['volume_24h'] >= volume]

    # Фильтрация по символу актива
    if search_value:
        df = df[df['symbol'].str.contains(search_value, case=False, na=False)]

    return df.to_dict('records')

# Коллбэк для обновления графика актива
@app.callback(
    Output('candlestick-chart', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('exchange-filter', 'value'),
     Input('market-type-filter', 'value')]
)
def update_chart(n, exchange, market_type):
    symbol = 'BTCUSDT'  # Заглушка для символа
    if exchange == 'Binance':
        df = get_data_binance(symbol)
    elif exchange == 'Bybit':
        df = get_data_bybit(symbol)
    elif exchange.lower() in ['okx', 'okex']:
        df = get_data_okx(symbol)
    else:
        df = pd.DataFrame()

    if df.empty:
        return go.Figure()  # Возвращаем пустой график, если данные отсутствуют

    candle = go.Candlestick(
        x=df['Open time'],
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        increasing_line_color='#00cc96',
        decreasing_line_color='#ef553b',
        name=f'{symbol} Price'
    )

    layout = go.Layout(
        xaxis=dict(
            title='Время (MSK)',
            showgrid=True,
            gridcolor='#444444',
        ),
        yaxis=dict(
            title=f'Цена {symbol} (USDT)',
            showgrid=True,
            gridcolor='#444444',
        ),
        plot_bgcolor='#1e1e1e',
        paper_bgcolor='#1e1e1e',
        font_color='#FFFFFF',
        margin=dict(l=50, r=50, t=50, b=50),
    )

    fig = go.Figure(data=[candle], layout=layout)

    return fig

# Callback to update the order book div
@app.callback(
    Output('order-book-div', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('exchange-filter', 'value')]
)
def update_order_book_div(n, exchange):
    symbol = 'BTCUSDT'  # Заглушка для символа
    if exchange == 'Binance':
        bids, asks = get_order_book_binance(symbol)
    elif exchange == 'Bybit':
        bids, asks = get_order_book_bybit(symbol)
    elif exchange == 'OKX':
        bids, asks = get_order_book_okx(symbol)
    else:
        bids, asks = pd.DataFrame(columns=['Price', 'Quantity']), pd.DataFrame(columns=['Price', 'Quantity'])

    bids['Side'] = 'Bids'
    asks['Side'] = 'Asks'
    order_book = pd.concat([bids, asks], ignore_index=True)
    order_book = order_book.sort_values(by='Price', ascending=False)

    rows = []
    for _, row in order_book.iterrows():
        color = '#00cc96' if row['Side'] == 'Bids' else '#ef553b'
        rows.append(
            html.Div([
                html.Span(f"{row['Price']:.2f}", style={'width': '33%', 'display': 'inline-block', 'color': color, 'fontWeight': 'bold'}),
                html.Span(f"{row['Quantity']:.6f}", style={'width': '33%', 'display': 'inline-block', 'color': color}),
                html.Span(f"{row['Side']}", style={'width': '33%', 'display': 'inline-block', 'color': color})
            ], style={'padding': '4px 0'})
        )

    return rows

if __name__ == '__main__':
    app.run_server(debug=True)