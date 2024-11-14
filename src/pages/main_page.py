# pages/main_page.py
from dash import dash_table, dcc, html, Input, Output
import dash_bootstrap_components as dbc
# src/pages/main_page.py
from src.app_instance import app
from src.main_logic import fetch_data_from_db, COLUMN_CONFIG, UNIFIED_COLUMNS


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

# Колбек для обновления таблицы на основе выбранных значений фильтров
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
