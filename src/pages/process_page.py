import os
import asyncio
import sys
import threading
from dash import callback_context
from src.app_instance import app
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

SCRIPTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
logs = []  # Глобальная переменная для логов выполнения

async def run_script_async(script_path):
    """Асинхронная функция для запуска скрипта с построчным чтением вывода."""
    global logs
    logs.append(f"Starting {script_path}...\n Please wait a moment, it's being executed.\n")

    process = await asyncio.create_subprocess_exec(
        sys.executable, "-u", script_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Построчное чтение stdout
    async for line in process.stdout:
        logs.append(line.decode('utf-8'))

    # Построчное чтение stderr
    async for line in process.stderr:
        logs.append(line.decode('utf-8'))

    await process.wait()

    # Проверяем код возврата для завершения
    if process.returncode == 0:
        logs.append(f"{script_path} completed successfully.\n")
    else:
        logs.append(f"{script_path} finished with an error, return code: {process.returncode}\n")

# Функция для запуска асинхронного процесса в отдельном потоке
def run_script_in_thread(script_path):
    asyncio.run(run_script_async(script_path))

# Определение макета страницы управления процессами
def process_management_page():
    return dbc.Container([
        dbc.Row([
            dbc.Col(html.H2("Process Management"), width=12)
        ]),
        dbc.Row([
            dbc.Col(dbc.Button("Update Main Data", id="update-main-btn", color="primary"), width="auto"),
            dbc.Col(dbc.Button("Update Options", id="update-options-btn", color="info"), width="auto"),
            dbc.Col(dbc.Button("Update OKX Futures", id="update-futures-okx-btn", color="warning"), width="auto"),
            dbc.Col(dbc.Button("Update OKX Trades", id="update-trades-okx-btn", color="danger"), width="auto"),
            dbc.Col(dbc.Button("Update Bybit Trades collect", id="update-trades-bybit-btn_1", color="success"), width="auto"),
            dbc.Col(dbc.Button("Update Bybit Trades full", id="update-trades-bybit-btn_2", color="success"), width="auto")
        ]),
        dbc.Row([
            dbc.Col(html.H3("Execution Log Console"), width=12)
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Textarea(  # Убрали компонент dcc.Loading для плавного обновления
                    id="console-output",
                    style={"width": "100%", "height": "300px", "backgroundColor": "#1e1e1e", "color": "#00FF00"}
                ),
                width=12
            )
        ]),
        dcc.Interval(id="console-update-interval", interval=5000, n_intervals=0)  # Периодическое обновление консоли
    ], fluid=True)


# Основной колбэк для запуска скриптов и обновления консоли
@app.callback(
    Output("console-output", "value"),
    [Input("update-main-btn", "n_clicks"),
     Input("update-options-btn", "n_clicks"),
     Input("update-futures-okx-btn", "n_clicks"),
     Input("update-trades-okx-btn", "n_clicks"),
     Input("update-trades-bybit-btn_1", "n_clicks"),
     Input("update-trades-bybit-btn_2", "n_clicks"),
     Input("console-update-interval", "n_intervals")],
    prevent_initial_call=True
)
def update_console_output(n_main, n_options, n_futures_okx, n_trades_okx, n_trades_bybit_1, n_trades_bybit_2, n_intervals):
    global logs
    triggered_id = callback_context.triggered[0]["prop_id"].split(".")[0]

    # Словарь с путями к скриптам
    script_map = {
        "update-main-btn": os.path.join(SCRIPTS_PATH, "main.py"),
        "update-options-btn": os.path.join(SCRIPTS_PATH, "opcion_modul.py"),
        "update-futures-okx-btn": os.path.join(SCRIPTS_PATH, "test_okx_futyres.py"),
        "update-trades-okx-btn": os.path.join(SCRIPTS_PATH, "okx_dradews_v5.py"),
        "update-trades-bybit-btn_1": os.path.join(SCRIPTS_PATH, "bybit_trads.py"),
        "update-trades-bybit-btn_2": os.path.join(SCRIPTS_PATH, "bybit_trades_cheker_2.py")
    }

    # Если колбэк вызван кнопкой запуска скрипта
    if triggered_id in script_map:
        script_to_run = script_map.get(triggered_id)
        if script_to_run and os.path.exists(script_to_run):
            logs.clear()  # Очищаем логи перед новым запуском
            logs.append(f"Starting {script_to_run}...\n Please wait a moment, it's being executed.\n")
            thread = threading.Thread(target=run_script_in_thread, args=(script_to_run,))
            thread.start()
        else:
            logs.append(f"Script {script_to_run} not found.\n")

    # Если колбэк вызван интервалом, просто возвращаем текущие логи
    return "\n".join(logs)
