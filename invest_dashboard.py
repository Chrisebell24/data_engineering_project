import os
import sys
import pandas as pd
from argparse import ArgumentParser
fp = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, fp)
from dash import Dash
from dash_table import DataTable
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input, State
from flask import Flask
import plotly.graph_objs as go

from data.tiger import get_price, get_tickers, get_fred_labels, get_fred_data, get_dividends

server = Flask(__name__)
app = Dash(
    __name__, 
    server=server, 
)

#app.title = 'AutoScripts'
app.layout = dcc.Tabs(
    value='Stock',
    children=[
        
        dcc.Tab(
            value='Stock',
            label='Stock',
            children=[
                html.H1('Select a Stock'),
                dcc.Dropdown(
                    id='dropdown_stock', 
                    options=[{'value': i, 'label': i} 
                    for i in get_tickers() ],
                ),
                html.Div(id='dropdown_stock_cache', style={'display': 'none'}),
                
                dcc.DatePickerRange(
                    id='date_range',
                    start_date= (pd.datetime.today().date()-pd.tseries.offsets.DateOffset(years=3)).strftime('%Y-%m-%d') ,
                    end_date = pd.datetime.today().strftime('%Y-%m-%d'),
                ),
                html.Div([
                    html.Button('submit', id='btn_submit'),
                    html.Div(id='btn_submit_cache', style={'display': 'none'}),
                ]),
                DataTable(id='tbl_info'),
                dcc.Graph(id='return_graph'),
                dcc.Graph(id='dividend_graph'),
        ]),
        
       dcc.Tab(
        value='Fed',
        label='Fed',
        children=[
            html.Div([
                html.H1('Select a Statistic'),
                dcc.Dropdown(id='dropdown_statistic'),
                dcc.Graph(id='graph_fed'),
            ]),
       ]),
    ])

@app.callback(
    [
        Output('graph_fed', 'figure'),
        Output('dropdown_statistic', 'options'),
    ],
    [
        Input('dropdown_statistic', 'value'),
    ],
    [
        State('dropdown_statistic', 'options'),
    ]
)
def update_statistics(dropdown_statistic, dropdown_statistic_options):
    
    if dropdown_statistic != None:
    
        data = get_fred_data(dropdown_statistic)
        
        trace1 = go.Scatter(
                x = pd.to_datetime(data['date']).dt.strftime('%Y-%m-%d'), 
                y = data['value'],
            )
            
        graph_item = {
            'data': [trace1],
            'layout': go.Layout(title=dropdown_statistic),
        }
        

    else:
        graph_item = {}

        dropdown_statistic_options = [{'value': i, 'label': i} for i in get_fred_labels() ]
        
    return graph_item, dropdown_statistic_options



@app.callback(
    [
        Output('btn_submit_cache', 'children'),
        Output('tbl_info', 'data'),
        Output('tbl_info', 'columns'),
        Output('date_range', 'start_date'),
        Output('date_range', 'end_date'),
        Output('dropdown_stock_cache', 'children'),
        Output('return_graph', 'figure'),
        Output('dividend_graph', 'figure'),
    ],
    [
        Input('btn_submit', 'n_clicks'),
    ],
    [
        State('btn_submit_cache', 'children'),
        State('dropdown_stock', 'value'),
        State('dropdown_stock_cache', 'children'),
        State('tbl_info', 'data'),
        State('tbl_info', 'columns'),
        State('date_range', 'start_date'),
        State('date_range', 'end_date'),
    ]
    
)
def dropdown_update(btn_submit, btn_submit_cache, dropdown_stock, dropdown_stock_cache,
    tbl_info_data, tbl_info_cols,
    date_range_start_date, date_range_end_date,
):
    print('dropdown_update: {}'.format(dropdown_stock))
    if dropdown_stock != None and btn_submit != None and btn_submit != btn_submit_cache:
        
        PRICES = get_price(
            dropdown_stock, 
            date_range_start_date, 
            date_range_end_date,
        )
        p = PRICES.xs(dropdown_stock, level=1, axis=1)
        close = p['close']
        ret = ((close-close.shift())/close.shift())
        cumret = (ret+1).cumprod()-1
        
        tbl_info_data = [{
            'symbol': dropdown_stock,
            'price': close.iloc[-1],
            'return': cumret.iloc[-1],
        }]
        
        tbl_info_cols = [{'name': i, 'id': i} for i in tbl_info_data[0].keys()]
        
        trace1 = go.Scatter(
            x = pd.to_datetime(cumret.index.astype(str)).strftime('%Y-%m-%d').tolist(), 
            y = cumret.values,
        )
        
        graph_item = {
            'data': [trace1],
            'layout': go.Layout(title='Cumulative Return'),
        }
        
        
        divs = get_dividends(dropdown_stock)
        
        trace1 = go.Scatter(
            x = divs.index, 
            y = divs.values,
        )
        
        dividends_item = {
            'data': [trace1],
            'layout': go.Layout(title=dropdown_stock+' Dividends'),
        }
        
        print('finished dropdown section')
    else:
        tbl_info_data, tbl_info_cols = [], []
        graph_item = {}
        dividends_item = {}
        
    return btn_submit, tbl_info_data, tbl_info_cols, str(date_range_start_date), str(date_range_end_date), \
    dropdown_stock, graph_item, dividends_item


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--port', dest='port', default=2002)
    args = parser.parse_args()
    app.run_server(debug=False, host='0.0.0.0', port=args.port)