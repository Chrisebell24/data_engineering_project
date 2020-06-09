import os
import sys
fp = os.path.dirname(os.path.dirname(os.path.abspath( __file__)))
sys.path.insert(0, fp)

import pandas as pd
import json
from util.sql_util import connect_to_default
CON = connect_to_default('tiger')
import warnings
warnings.filterwarnings('ignore')

def get_dividends(stock):
    sql = '''
    SELECT date, dividends FROM dividends where ticker = '{}'
    '''.format(stock)
    return pd.read_sql(sql, CON).sort_values('date').set_index('date')['dividends']

def get_fred_data(measure):
    sql = '''
    SELECT date, value FROM fred where field = '{}'
    '''.format(measure)
    return pd.read_sql(sql, CON).sort_values('date')

def get_fred_labels():
    return pd.read_sql('SELECT distinct field FROM fred', CON).unstack().tolist()

def get_tickers():
    return pd.read_sql('SELECT ticker FROM exch_nyse where active = 1', CON).unstack().tolist()

def get_cboe(product, primary_only=True, pivot=False, max_t_on_pivot=4):
    
    expiration = 'expiration'
    trade_date = 'trade_date'
    df = pd.read_sql("SELECT * from cboe where product='{product}'".format(product=product), CON)
    df[expiration] = pd.to_datetime(df[expiration])
    df[trade_date] = pd.to_datetime(df[trade_date])
    df.sort_values([trade_date, expiration], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['t'] = (df[expiration]-df[trade_date]).dt.days
    df['min_t'] = df.groupby(expiration)[trade_date].transform('min')
    df['primary'] = df.groupby('futures')['min_t'].transform('min')==df['min_t']
    df.drop('min_t', axis=1,inplace=True)
    
    if primary_only:
        df = df[df['primary']]
        
    df = df[df['close']>0]
    
    if pivot:
        data = []
        for td, grp in df.groupby('trade_date'):
            grpf = grp[grp['t']>=max_t_on_pivot]
            data.append((
                td,
                grpf['expiration'].iloc[0],
                grpf['t'].iloc[0],
                (grpf['close'].shift()-grpf['close']).iloc[1],
            ))

        data = pd.DataFrame(data, columns=['trade_date', 'expiration', 't', 'fs'])
        datap = data.pivot(columns='expiration', index='t', values='fs').sort_index().iloc[:,:-1].iloc[:,1:].fillna(method='bfill').dropna()
        datap.columns=[i.strftime('%Y-%m-%d') for i in datap.columns]
        return datap
    
    return df

def get_price(symbols, start_date=None, end_date=None):
    '''
    Parameters
    ----------
    symbols : str/list/pd.Series
        list of symbols
    start_date : None, str/datetime
        start date
    end_date : None, str/datetime
        end date
    
    Returns
    -------
    pandas DataFrame of prices
    '''
    if type(symbols) is str: symbols = [symbols]
    if type(symbols) is pd.Series: symbols = symbols.tolist()

    sql_str = ''

    if start_date != None:
        sql_str+=" date >= '{}'".format(pd.to_datetime(start_date).strftime('%Y%m%d'))

    if end_date != None:
        sql_str+=" and date <= '{}'".format(pd.to_datetime(end_date).strftime('%Y%m%d'))

    sql='SELECT ticker,date,open,high,low,close,volume from prices WHERE'+sql_str
    
    if symbols in [None, '*']:
        data = pd.read_sql(sql, CON)
    
    else:
        step = 20
        if len(symbols) > step:
        
            dflist = []
            sql_base = sql
            for i in range(0, len(symbols), step):
                tickers_slice = symbols[i:min(len(symbols), i+step)]
                sql = sql_base + " ticker in ('{}')".format("','".join(symbols))
                data = pd.read_sql(sql, CON)
                dflist.append(data)
                
            data = pd.concat(dflist)
            
        else:    
            data = pd.read_sql(sql, CON)
        
    for col in ['open','high','low','close','volume']:
        data[col] = pd.to_numeric(data[col], errors='ignore')
    
    cols = [i for i in data.columns if i not in ['date','ticker']]

    data = data.pivot_table(
        index='date', 
        columns='ticker', 
        values=cols,
    )
    return data
    found_columns = list(set([i[1] for i in data.columns]))
    data = data[['open','high','low','close', 'volume']]
    data = data.reorder_levels([1,0],axis=1)
    data = data[[i for i in symbols if i in found_columns]]
    data.columns.names = [None, None]

    if len(data.columns.levels[0]) == 1:
        data = data.droplevel(level=0, axis=1)
        data = data[['open','high','low','close', 'volume']]
        
    data.index.name='Date'
    return data

def get_holdings(symbol, date=None, source=None):
    '''
    Parameters
    ----------
    symbol : str
        ETF ticker
    date : str/datetime
        Date to reference
    source : None, str
        database to pull from:
            ishares
            zacks
        
    Returns
    -------
    pandas dataframe of ishares holdings
    '''
    if date is None: date = pd.datetime.today().date()

    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    
    if source is None:
        df = _get_holdings(symbol=symbol, date=date, source='ishares')
        if df.empty:
            df = _get_holdings(symbol=symbol, date=date, source='zacks')
            return df
        else:
            return df
    
    sql = '''
    SELECT ticker,asset_class,sum(shares) as 'shares' 
    FROM tiger.{source} 
    WHERE date<='{date}' and etf_symbol={symbol}
    group by ticker,asset_class;
    '''.format(symbol=symbol, date=date, source=source)

    data = pd.read_sql(sql, CON)
    return data
