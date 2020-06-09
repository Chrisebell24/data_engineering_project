import pandas as pd
import os
import sys

#fp = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#sys.path.insert(0,fp)

from _sql_util import connect_to_default

def update_exch(df, tbl, ticker_col=None):    
    
    today_str_date = pd.datetime.today().strftime('%Y-%m-%d')
    df['date'] = today_str_date
    df.columns = [i.replace(' ','_').lower() for i in df.columns]
    df['active'] = 1
    
    if ticker_col != None:
        print('renaming columns')
        df.rename(columns={ticker_col.replace(' ','_').lower(): 'ticker'}, inplace=True)
    
    con = connect_to_default('tiger')
    
    try:
        current = pd.read_sql_table(tbl, con)
        active_tickers = current[current['active']==1]['ticker'].tolist()
    except:
        active_tickers = []
        
    #inactive_tickers = current[current['active']!=1]['ticker'].tolist()
    
    if not df.empty:
        ticker_list = df['ticker'].tolist()
        df = df[~df['ticker'].isin(active_tickers)]
        df.to_sql(tbl, con, if_exists='append', index=False)
        
        for t in active_tickers:
            if t not in ticker_list:
                # deactivate
                sql = '''
                SELECT * from {}
                WHERE ticker='{}';
                '''.format(tbl, t)

                data = pd.read_sql(sql, con)

                data['date'] = pd.datetime.today().strftime('%Y-%m-%d')
                data['active'] = 0
                data.to_sql(tbl, con, if_exists='append', index=False)
