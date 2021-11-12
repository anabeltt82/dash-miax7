import pandas as pd
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go

import api_handler

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

ah = api_handler.APIBMEHandler(market='IBEX')

app.layout = html.Div(children=[
    html.H1(
        children='MIAX Data Explorer 2',
    ),
    html.H5(
        children='mIAx API',
    ),
    dcc.Dropdown(
        id='markets',
        options=[
            {'label': 'IBEX', 'value': 'IBEX'},
            {'label': 'DAX', 'value': 'DAX'},
            {'label': 'EUROSTOXX', 'value': 'EUROSTOXX'}
        ],
        value='IBEX'
    ),
    dcc.Dropdown(
        id='tickers',
    ),
    dcc.Graph(
        id='graph',
    )
])


@app.callback(
    Output('tickers', 'options'),
    Input('markets', 'value'))
def change_index(selected_index):
    ah.market = selected_index
    ticker_master = ah.get_ticker_master()
    tcks = list(ticker_master.ticker)
    dropdown_values = [{'label': tck, 'value': tck} for tck in tcks]
    return dropdown_values


@app.callback(
    Output('tickers', 'value'),
    Input('tickers', 'options'))
def change_value_ticker(new_options):
    return new_options[0]['value']


@app.callback(
    Output('graph', 'figure'),
    Input('tickers', 'value'))
def change_graph(ticker):
    ticker_data = ah.get_data_ticker(ticker=ticker)
    # fig = px.line(ticker_data)
    fig = go.Figure(go.Candlestick(
        x=ticker_data.index,
        open=ticker_data['open'],
        high=ticker_data['high'],
        low=ticker_data['low'],
        close=ticker_data['close']
    ))
    return fig

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", debug=False, port=8080)

