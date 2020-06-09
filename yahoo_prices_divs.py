import os
import sys

#fp = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#sys.path.insert(0, os.path.join(fp, 'data_capture', 'yhoo'))
#sys.path.insert(0, os.path.join(fp, 'util'))

from _sql_util import connect_to_default, to_sql

from _yfin_download import download

import pandas as pd
import datetime as dt

def get_tickers(con):
    tickers = pd.read_sql('SELECT ticker from exch_nyse where active=1', con).unstack().tolist()
    return tickers
    
def main():

    con = connect_to_default(database='tiger')

    tickers = get_tickers(con)

    if 'prices_register' in con.table_names():
        max_grp = pd.read_sql_table('prices_register', con)
        max_grp.set_index('ticker', inplace=True)
        max_grp_df = max_grp.copy()
        max_grp = max_grp['date'].to_dict()
        
        
    else:
        max_grp = {}

    if 'dividends_register' in con.table_names():
        max_grp_div = pd.read_sql_table('dividends_register', con)
        max_grp_div.set_index('ticker', inplace=True)
        max_grp_div = max_grp_div['date'].to_dict()

    else:
        max_grp_div = {}

    start_date = pd.to_datetime('2000-01-01')
    end_date = pd.datetime.today().date()

    for t in tickers:
        try:
            sd = pd.to_datetime(str(max_grp.get(t, start_date)))
            ed = end_date
            print('downloading prices for: {} {} {}'.format(t, sd, ed))
            
            if sd < ed:
                try:
                    prices = download(t, sd, end_date).dropna()
                except:
                    print('Error: {}'.format(t))
                    continue
                
                if not prices.empty:
                    if 'Adj Close' in prices.columns: prices = prices.drop('Adj Close', axis=1)
                    prices = prices.round(2).reset_index()
                    prices.columns = [col.lower().replace(' ','_') for col in prices.columns]
                    prices['date'] = pd.to_datetime(prices['date']).dt.strftime('%Y%m%d').astype(int)
                    prices['ticker'] = t        
                    
                    if t in max_grp:
                    
                        # check if need to frag and re-pull
                        close_price = max_grp_df[max_grp_df['date']==max_grp.get(t)].loc[t]['close']
                        close_price_prices = prices[prices['date'] == max_grp.get(t)]['close'].iloc[0]
                        
                        print('{} - {} close price: {}'.format(t, max_grp.get(t), close_price) )
                        print(close_price, close_price_prices)
                        if close_price != close_price_prices:
                            # delete database and re-pull - adjustment
                            print('Adjustment needed: re-pulling {}'.format(t))
                            prices = download(t, start_date, end_date)
                            if 'Adj Close' in prices.columns: prices = prices.drop('Adj Close', axis=1)
                            prices = prices.round(2).reset_index()
                            prices.columns = [col.lower().replace(' ','_') for col in prices.columns]
                            prices['date'] = pd.to_datetime(prices['date']).dt.strftime('%Y%m%d').astype(int)
                            prices['ticker'] = t        
                            
                            sql = '''
                            DELETE from prices where ticker = '{}'
                            '''.format(t)
                            con.execute(sql)
                            
                            prices.to_sql('prices', con, if_exists='append', index=False)
                            
                        else:
                            prices[prices['date'] > max_grp.get(t) ].to_sql('prices', con, if_exists='append', index=False)
                    else:
                        prices.to_sql('prices', con, if_exists='append', index=False)
                    
                    pr = pd.DataFrame(prices.loc[prices['date'].idxmax()][['ticker','date', 'close']]).T
                    if not t in max_grp: 
                    
                        if not 'prices_register' in con.table_names():
                            sql = '''CREATE TABLE prices_register (
                                ticker varchar(15) NOT NULL,
                                date int,
                                close decimal(12,2)
                            )'''
                            con.execute(sql)
                    
                        sql = "INSERT INTO prices_register (date, ticker, close) VALUES ({},'{}',{})".format(
                            int(pr['date'].iloc[0]), 
                            pr['ticker'].iloc[0],
                            float(pr['close'].iloc[0])
                        )
                        con.execute(sql)
                    else:
                        # update
                        sql = "UPDATE prices_register SET date = {} where ticker = '{}'".format(int(pr['date'].iloc[0]), pr['ticker'].iloc[0])
                        con.execute(sql)
                        
                        sql = "UPDATE prices_register SET close = {} where ticker = '{}'".format(int(pr['close'].iloc[0]), pr['ticker'].iloc[0])
                        con.execute(sql)
            
            
            sd = pd.to_datetime(str(max_grp_div.get(t, start_date)))+dt.timedelta(1)
            ed = end_date
            print('downloading dividends for: {} {} {}'.format(t, sd, ed))
            
            if sd < ed:
                
                try:
                    dividends = download(t, sd, ed, events='div').reset_index().dropna()
                except:
                    print('ERROR divs {}'.format(t))
                    continue
                    
                if not dividends.empty:
                    dividends.columns = [col.lower().replace(' ','_') for col in dividends.columns]
                    dividends['ticker'] = t
                    if 'date' in dividends.columns: dividends.sort_values('date', inplace=True)
                    dividends['date'] = pd.to_datetime(dividends['date']).dt.strftime('%Y%m%d').astype(int)
                    dividends.reset_index(inplace=True, drop=True)        
                    
                    if t in max_grp_div:
                        dividends[dividends['date']>max_grp_div.get(t)].to_sql('dividends', con, if_exists='append', index=False)
                    else:
                        dividends.to_sql('dividends', con, if_exists='append', index=False)
                        
                        
                    dr = pd.DataFrame(dividends.loc[dividends['date'].idxmax()][['ticker','date']]).T
                    if not t in max_grp_div: 
                    
                        if not 'dividends_register' in con.table_names():
                            sql = '''CREATE TABLE dividends_register (
                                ticker varchar(15) NOT NULL,
                                date int
                            )'''
                            con.execute(sql)
                    
                    
                        sql = "INSERT INTO dividends_register (date, ticker) VALUES ({},'{}')".format(int(dr['date'].iloc[0]), dr['ticker'].iloc[0])
                        con.execute(sql)
                    else:
                        # update
                        sql = "UPDATE dividends_register SET date = {} where ticker = '{}'".format(int(pr['date'].iloc[0]), pr['ticker'].iloc[0])
                        con.execute(sql)
        except Exception as e:
            print('ERROR: {}'.format(e))
            
if __name__ == '__main__':
    main()
