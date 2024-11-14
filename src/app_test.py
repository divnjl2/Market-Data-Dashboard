# src/app.py
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
from src.app_instance import app
from src.pages import main_page, process_page
# app_test.py
import sys
import os

# Добавляем путь к `src` в пути поиска Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


app.layout = dbc.Container([
    dcc.Location(id="url"),
    dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Главная", href="/")),
            dbc.NavItem(dbc.NavLink("Управление процессами", href="/process-management"))
        ],
        color="light",
        dark=False
    ),
    html.Div(id="page-content")
], fluid=True)

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname):
    if pathname == "/process-management":
        return process_page.process_management_page()
    else:
        return main_page.main_page_layout()

if __name__ == "__main__":
    app.run_server(debug=True)
