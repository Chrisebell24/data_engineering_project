import re
import requests
import sys
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pdb import set_trace as pb

max_fallback = 2

class Currency:
    
    def __init__(self):
        self.data = {}
        self.data_hist = {}
        
    def get(self, currency_pair):
        '''
        Parameters
        ----------
        currency_pair : str 
        
        Returns
        -------
        dictionary of the currency pair
        '''
        
        if currency_pair not in self.data:
            
            curr = get_historical_currency(currency_pair)
            self.data[currency_pair] = curr.T.to_dict()[curr.index[0]]
            
        return self.data[currency_pair]
    
    def get_hist(self, currency_pair, dates):
        
        if currency_pair not in self.data_hist:
            
            self.data_hist[currency_pair] = get_historical_currency(currency_pair, dates)
            
        return self.data_hist[currency_pair]
    
    def fill(self):
        '''
        Fill entire data cross pair
        '''
        if self.data == {}: self.get('USD')
        
        i = self.data.keys()[0]
        for k in self.data[i].keys():
            self.get(k)


def get_historical_currency(base, date=pd.datetime.today().strftime('%Y-%m-%d')):
    '''
    Parameters
    ----------
    base : str
        currency base
    date : str/datetime - list
        list of dates
    
    Returns
    -------
    pandas dataframe of currency pairs
    
    Example
    -------
    get_historical_currency(
        'USD', 
        pd.bdate_range('2017-01-03', '2019-01-04')
    )
    '''
    
    if type(date) in [list, pd.Series, pd.core.indexes.datetimes.DatetimeIndex]:        
        return pd.concat([get_historical_currency(base=base, date=d) for d in date]).sort_index()
    
    
    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    url = 'https://www.xe.com/currencytables/?from={base_currency}&date={date}'.format(
        base_currency=base,
        date=date
    )
    
    count = 0
    while count<=10:
        try:
            curr = pd.read_html(url)
            assert curr.shape[1] >=4
            break
        except:
            count+=1
            
    curr = curr[0].iloc[:,]
    curr['date'] = date
    try:
        curr = curr.iloc[:,[4,0,2]]
    except:
        print(curr)
        print(date)
        assert False
        
    curr.columns=['date','currency','value']
    curr = curr.pivot_table(values='value', index='date', columns='currency')
    return curr

def _clean_bb_ticker(symbol, fallback):
    
    if fallback == 0:
        exchange_dict = {
            'CN': 'TO',
            'AU': 'AX',
            'HK': 'HK',
            'LN': 'L',
            'TI': 'IS',
            'SW': 'SW',
            'US': None,
        }
    elif fallback == 1:
        exchange_dict = {
            'CN': 'V',
        }
        
    else:
        exchange_dict = {}
    
    symbol = symbol.upper()
    symbol = symbol.replace(' EQUITY', '')
    
    str_split = symbol.split(' ')
    if len(str_split)==1: return symbol
    
    symb, exchange = str_split
    
    if exchange.upper() in exchange_dict:
        correct_symbol = exchange_dict[exchange.upper()]
    else:
        print('Did not find symbol: {} in exchange_dict ({})'.format(exchange.upper(), symb))
        correct_symbol = exchange.upper()
    
    if correct_symbol != None:
        symbol = symb+'.'+correct_symbol
    else:
        symbol = symb
    
    return symbol


def statistics(symbols, currency=None, date=None, **args):
    '''
    Parameters
    ----------
    symbols : str/list/pd.Series
        symbols
    convert_currency : None - str
        convert to currency
        e.g. ['USD', 'IDR', 'GBP', 'ETH', 'CAD', 
        'JPY', 'HUF', 'MYR', 'SEK', 'SGD', 'HKD',
        'AUD', 'CHF', 'CNY', 'NZD', 'THB', 'EUR',
        'RUB', 'INR', 'MXN', 'BTC', 'PHP', 'ZAR']
    date : None, str/datetime
        convert market cap and other price measures to 
        a previous date. Does not adjust for share count
        changes
    
    Returns
    -------
    pandas dataframe of stats from ticker
    '''
    
    convert_currency = currency
    
    if '_curr' in args: 
        curr = args['_curr']
    else:
        curr = None
        
    
    if type(symbols) in [list, pd.Series, set]:
        global _currency
        _currency = Currency()
        
        return pd.concat([statistics(symb, currency=currency) for symb in symbols], sort=True)
    
    elif not '_currency' in globals():
        _currency = Currency()
        
    
    if 'fallback' in args:
        fallback = args['fallback']
    else:
        fallback = 0
    
    ticker = _clean_bb_ticker(symbols, fallback)
    url = 'https://finance.yahoo.com/quote/{ticker}/key-statistics'.format(
        ticker=ticker
    )

    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'lxml')
    
    main = soup.find_all('tr')

    data = {}
    dig_dict = {'B': 1000000000,'M': 1000000,'K': 1000}

    for i in main:
        table_cells = i.find_all('td')

        if len(table_cells)==2:
            k, v = table_cells

            k = str(k.find_all('span')[0].getText())
            
            try:
                v = str(v.getText())
            except:
                v = pd.np.nan

            try: 
                pd.to_datetime(v)
                isdate = True
            except:
                isdate = False
            
            try:
                if v == pd.np.nan:
                    pass

                elif str(v[-1]).upper() in dig_dict and str(v[:-1]).replace(',','').replace('.','').replace('-','').isdigit():
                    v = float(v[:-1])*dig_dict[v[-1].upper()]

                elif (str(v[-1]) == '%') and (str(v)[:-1].replace(',','').replace('.','').replace('-','').isdigit()):
                    v = float(v[:-1])*1.0/100.0

                elif (str(v).replace(',','').replace('.','').replace('-','').isdigit()):
                    v = float(v)

                elif isdate:
                    v = pd.to_datetime(v).date().strftime('%Y-%m-%d')
            except:
                pass

            data[k] = v
    
    if data == {} and 'retry' not in args and fallback < max_fallback:
        
        fallback += 1
        data = statistics(symbols, fallback=fallback)
        data.index = [symbols]
        
    elif data == {} and 'retry' not in args:
        data = statistics(symbols.split(' ')[0]+' Equity', retry=True)
        
    else:
        data = pd.DataFrame([data], index=[symbols])
    
    
    if 'local_currency' not in data.columns:
        
        spans = [i for i in soup.find_all('span') if 'Currency in' in i.get_text()]
        spans = [i.get_text().split('Currency in ')[-1] for i in spans]
        if spans!=[]:
            data['local_currency'] = spans[0]
        else:
            data['local_currency'] = None
    
    if convert_currency != None:
        
            
        currency_divider  = []
        for iid, row in data.iterrows():
            curr = _currency.get(row['local_currency'])
            currency_divider.append(1/curr[convert_currency])
            
        data['currency_divider'] = currency_divider
        
        for col in ['EBITDA', 'Gross Profit', 'Levered Free Cash Flow', 'Market Cap (intraday)', 'Revenue',
'Operating Cash Flow', 'Revenue Per Share', 'Gross Profit', 'Net Income Avi to Common',
'Diluted EPS', 'Total Cash', 'Total Cash Per Share', 'Total Debt']:
            if col in data.columns: 
                data[col] = pd.to_numeric(data[col].replace('N/A', np.nan), errors='ignore')/data['currency_divider']
                
    if date != None:
        prices = download(symbol=symbols, start_date=pd.to_datetime(date), end_date=pd.datetime.today().date())
        multiplier = prices['Close'].iloc[0]/prices['Close'].iloc[-1]
        
        for col in ['Market Cap (intraday)']:
            if col in data.columns:
                data[col]*=multiplier
                
            
    return data

def get_currency(ticker):
    '''
    Parameters
    ----------
    ticker : str
        ticker
    
    Returns
    -------
    currency that the ticker is priced in
    '''
    return statistics(ticker)['local_currency'].iloc[0]
    

def download(symbol, start_date, end_date, interval='1d', events='history', currency=None, **args):
    '''
    Parameters
    ----------
    symbol : str/list/pd.Series
        list of symbols
    start_date : str/datetime
        start date
    end_date : str/datetime
        end date
    interval : str
        '1d'
    events : str
        'history', 'div'
    currency : str
        currency to convert to
    
    Returns
    -------
    pandas dataframe of prices
    
    Example
    -------
    df = get_prices('AAPL', '2019-01-01', '2019-01-31')
    '''
    
    if 'fallback' in args:
        fallback = args['fallback']
    else:
        fallback = 0
    
    if type(symbol) is pd.Series:
        symbol = symbol.tolist()
    
    if '_currency' in args:
        _currency = args['_currency']
    else:
        _currency = Currency()
        if currency != None:
            dates = pd.bdate_range(start_date, end_date)
            _currency.get_hist(currency.upper(), dates)
    
    if type(symbol) is list:
        
        output = {}
        for symb in symbol:
            output[symb] = download(
                symbol=symb,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                events=events,
                currency=currency,
                _currency=_currency,
            )
        
        comb = pd.concat(output, axis=1, sort=True)
        comb.columns.names=[None, None]
        comb.index.name='Date'
        
        return comb
    
    if not '_currency' in globals(): _currency = Currency()
    
    symbol = _clean_bb_ticker(symbol, fallback)
    
    sd = pd.to_datetime(start_date)
    sd = ((sd - pd.to_datetime('1970-01-01')).days)*24*60*60
    
    ed = pd.to_datetime(end_date)
    ed = ((ed - pd.to_datetime('1970-01-01')).days)*24*60*60
    
    crumble_link = 'https://finance.yahoo.com/quote/{0}/history?p={0}'
    crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
    cookie_regex = r'set-cookie: (.*?);'
    quote_link = 'https://query1.finance.yahoo.com/v7/finance/download/{}?period1={}&period2={}&interval={}&events={}&crumb={}'

    link = crumble_link.format(symbol)
    session = requests.Session()
    
    proxy = '{}.{}.{}:{}'.format(
        pd.np.random.randint(10,99),
        pd.np.random.randint(10,99), 
        pd.np.random.randint(0,9),
        pd.np.random.randint(10,999),
        pd.np.random.randint(1000,9999),
    )
    
    response = session.get(link, proxies={'http': 'http://{}'.format(proxy)})

    # get crumbs

    text = str(response.content)
    match = re.search(crumble_regex, text)
    try:
        crumbs = match.group(1)
    except:
        return pd.DataFrame()

    # get cookie

    cookie = session.cookies.get_dict()

    url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s" % (
        symbol, sd, ed, interval, events, crumbs
    )
    
    count = 0
    while count<=10:
        try:
            r = requests.get(url,cookies=session.cookies.get_dict(), timeout=10, stream=True)
            break
        except:
            count+=1
        
    if count >= 10: raise ValueError('Timeout Error on {}'.format(url))
    
    out = r.text

    df = pd.DataFrame([tuple(i.split(',')) for i in out.split('\n')])
    df.columns = df.iloc[0]
    df=df.iloc[1:].dropna()
        
    if not 'skip_timeout' in args:
        
        while '{' in df.columns:

            df = download(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                events=events,
                currency=currency,
                _currency=_currency,
                skip_timeout = True,
            )
    try:
        df.set_index('Date', inplace=True)
    except:
        pass
    
    for col in df.columns:
        df[col] = pd.to_numeric(df[col].replace('null', pd.np.nan), errors='ignore')
    
    
    if df.empty and 'retry' not in args and fallback < max_fallback:
        
        fallback += 1
        df = download(
            symbol=symbol,
            fallback=fallback,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            events=events,
            currency=currency,
            _currency=_currency,
        )
        
    elif df.empty == {} and 'retry' not in args:
        df = download(
            symbol=symbol.split(' ')[0]+' Equity',
            fallback=fallback,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            events=events,
            currency=currency,
            _currency=_currency,
            retry=True,
        )
    
    if currency != None:
        symb_currency = get_currency(ticker=symbol)
        
        if symb_currency != currency:
            currency_table = _currency.get_hist(currency, pd.bdate_range(start_date, end_date))
            currency_series = currency_table[[symb_currency]]
            df = df.merge(currency_series, left_index=True, right_index=True)
            
            for col in df.columns:
                if col not in ['Volume', symb_currency]:
                    df[col] = df[col]/df[symb_currency]

            df.drop([symb_currency], axis=1, inplace=True)
        
    return df

def get_summary(symbols, **args):
    '''
    Parameters
    ----------
    symbols : str/list/series
    
    Returns
    -------
    pandas dataframe of summary statistics
    '''
    
    if type(symbols) in [list, pd.Series, set]:
        
        return pd.concat([get_summary(symb) for symb in symbols], sort=True)
    
        
    if 'fallback' in args:
        fallback = args['fallback']
    else:
        fallback = 0
    
    ticker = _clean_bb_ticker(symbols, fallback)
    url = 'https://finance.yahoo.com/quote/{ticker}'.format(
        ticker=ticker
    )

    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'lxml')
    
    main = soup.find_all('tr')

    data = {}
    dig_dict = {'B': 1000000000,'M': 1000000,'K': 1000}

    for i in main:
        table_cells = i.find_all('td')

        if len(table_cells)==2:
            k, v = table_cells

            k = str(k.find_all('span')[0].getText())
            
            try:
                v = str(v.getText())
            except:
                v = pd.np.nan

            try: 
                pd.to_datetime(v)
                isdate = True
            except:
                isdate = False
            
            try:
                if v == pd.np.nan:
                    pass

                elif str(v[-1]).upper() in dig_dict and str(v[:-1]).replace(',','').replace('.','').replace('-','').isdigit():
                    v = float(v[:-1])*dig_dict[v[-1].upper()]

                elif (str(v[-1]) == '%') and (str(v)[:-1].replace(',','').replace('.','').replace('-','').isdigit()):
                    v = float(v[:-1])*1.0/100.0

                elif (str(v).replace(',','').replace('.','').replace('-','').isdigit()):
                    v = float(v)

                elif isdate:
                    v = pd.to_datetime(v).date().strftime('%Y-%m-%d')
            except:
                pass

            data[k] = v
    
    if data == {} and 'retry' not in args and fallback < max_fallback:
        
        fallback += 1
        data = get_summary(symbols, fallback=fallback)
        data.index = [symbols]
        
    elif data == {} and 'retry' not in args:
        data = get_summary(symbols.split(' ')[0]+' Equity', retry=True)
        
    else:
        data = pd.DataFrame([data], index=[symbols])
        
    return data
