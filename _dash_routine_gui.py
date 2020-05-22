import sys
import os
import numpy as np
import pandas as pd
from dash import Dash
from dash_table import DataTable
from flask import Flask, send_from_directory, url_for
from argparse import ArgumentParser
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input, State
import dash_html_components as html


fp = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
fp = os.path.join(fp, 'util')
sys.path.insert(0, fp)

from sql_util import connect_to_default

global con
con = connect_to_default('tiger')

server = Flask(__name__)
app = Dash(
    __name__, 
    server=server, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)

app.title = 'AutoScripts'
app.layout = html.Div([

    dbc.Jumbotron([
        html.H1('Automated Python Scripts'),
        
        dbc.Button('Save', id='btn_submit'),
        html.Div(id='btn_submit_cache', style={'display': 'none'}),
        html.Div(id='btn_submit_lbl'),
        
        DataTable(
            id='form_table', 
            columns=[{'name': 'fp', 'id': 'fp'}, {'name': 'val', 'id': 'val', 'presentation': 'dropdown' } ], 
            editable=True,
            
            dropdown={
                'val': {
                    'options': [
                        {'label': str(i), 'value': str(i)}
                        for i in [True, False]
                    ]
                }
            },
            
        style_table={'display' : 'block !important'},
        
        ),
        
        dbc.Button('Run Single', id='btn_run'),
        html.Div(id='btn_run_cache', style={'display': 'none'}),
        html.Div(id='btn_run_lbl'),
        
        dbc.Button('Run All', id='btn_run_all'),
        html.Div(id='btn_run_all_cache', style={'display': 'none'}),
        html.Div(id='btn_run_all_lbl'),
        
    ]),
])


@app.callback(
    [
        Output('btn_run_lbl', 'children'),
        Output('btn_run_cache', 'children'),
    ],
    [
        Input('btn_run', 'n_clicks'),
    ],
    [
        State('btn_run_cache', 'children'),
        State('form_table', 'data'),
        State('form_table', 'active_cell'),
    ]
)
def run(btn_run, btn_run_cache, form_table, form_table_active_cell):
    msg = ''
    if btn_run != None and btn_run > 0 and btn_run != btn_run_cache and form_table_active_cell.get('column_id', None) == 'fp':
    
        fp = os.path.dirname( os.path.abspath( __file__))
        val = form_table[ form_table_active_cell['row'] ]['fp']
        
        val = os.path.join(fp, val)
        print('running single: {}'.format(val))
        
        command = "python {}.py".format(val)
        print('running: {}'.format(command))
        try:
            os.system(command)
            msg = 'Success: {}'.format(command)
        except Exception as e:
            msg = 'ERROR: {}'.format(e)
            
    return msg, btn_run
    
@app.callback(
    [
        Output('btn_run_all_lbl', 'children'),
        Output('btn_run_all_cache', 'children'),
    ],
    [
        Input('btn_run_all', 'n_clicks'),
    ],
    [
        State('form_table', 'data'),
        State('btn_run_all_cache', 'children'),
    ]
)
def run_all(btn_run_all, form_table, btn_run_all_cache):

    msg = ''
    if btn_run_all != None and btn_run_all > 0 and btn_run_all != btn_run_all_cache:
        msg = 'Run all complete'
        
        fp = os.path.dirname( os.path.abspath( __file__))
        
        
        for row in form_table:
            
            if str(row['val']) in [True, 'True', 1]:
                print('running: {}'.format( row['fp'] ))
                
                val = os.path.join(fp, row['fp'])
                command = "python {}.py".format(val)
                print('running: {}'.format(command))
                
                try:
                    os.system(command)

                except Exception as e:
                    print('ERROR')
                    print(command)
                    print(e)
                    
                
    return msg, btn_run_all
    
@app.callback(
    [
        Output('btn_submit_lbl', 'children'),
        Output('btn_submit_cache', 'children'),
        Output('form_table', 'data'),
    ],
    [
        Input('btn_submit', 'n_clicks'),
    ],
    [
        State('form_table', 'data'),
        State('btn_submit_cache', 'children'),
    ]
)
def save_connection(btn_submit, form_table, btn_submit_cache):
    global con
    msg = ''
    if btn_submit!=None and btn_submit > 0 and btn_submit != btn_submit_cache:
        
        msg = 'Saving new data'
        
        sql = '''
        SELECT fp,val from routine_settings where field = 'active'
        '''
        df = pd.read_sql(sql, con)
        
        for j in form_table:
            dfslice = df[df['fp'] == j['fp'] ]
            val = {'True': 1, 'False': 0}[j['val']]
            
            
            if dfslice.empty:
                # insert new
                sql = '''
                INSERT INTO routine_settings (fp, field, val) VALUES ('{}', 'active', 0)
                '''.format(j['fp'])
                
                try:
                    con.execute(sql)
                except:
                    pass
                
            else:
                orig_val = dfslice['val'].iloc[0]
                
                if val != orig_val:
                    
                    sql = '''
                    UPDATE routine_settings
                    SET val = {}
                    WHERE fp = '{}' and field = 'active'
                    '''.format( val, j['fp'] )
                    
                    try:
                        con.execute(sql)
                    except:
                        pass
        
        
    else:
        #init script
        sql = '''
        SELECT fp,val from routine_settings where field = 'active'
        '''
        df = pd.read_sql(sql, con)
        
        if df.empty:
            form_table = []
        else:
            df['val'] = np.where(df['val'] == 1, 'True', 'False')
            form_table = df[['fp', 'val']].to_dict('records') 
        
        df.sort_values('fp', inplace=True)
        
        files = [
            {'fp': i.split('.')[0], 'val': 0} for i in os.listdir(os.path.dirname(os.path.abspath(__file__)))
            if i.split('.')[0] != 'routines' and i[0] != '_' and '.py' in i and not i.split('.')[0] in df['fp'].tolist()
        ]
        
        
        if len(files) > 0: 
            form_table.extend(files)
        
        
        
        
    return msg, btn_submit, form_table

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--port', dest='port', default=2001)
    
    if not 'routine_settings' in con.table_names():
        
        sql = '''
        CREATE TABLE routine_settings (
            fp varchar(225),
            field varchar(255),
            val int
        )
        '''
        print(sql)
        con.execute(sql)
    
    args = parser.parse_args()
    app.run_server(debug=True, host='0.0.0.0', port=args.port)