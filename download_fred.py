APIKEY = 'fc3d944885af80bfd0a78cad14e331f4'
DATABASE='tiger'
import os
import sys
import pandas as pd
from fredapi import Fred

#fp = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#fp = os.path.join(fp, 'util')
#sys.path.insert(0, fp)
from _sql_util import connect_to_default

def update_fred():
    
    con = connect_to_default(DATABASE)
    fred = Fred(api_key=APIKEY)

    fred_series_list = ['GDPA',
     'BASE',
     'DEXUSEU',
     'DGS10',
     'BAMLH0A0HYM2',
     'SP500',
     'AAA',
     'BAA',
     'CIVPART',
     'CPIAUCSL',
     'CURRCIR',
     'EXUSEU',
     'FEDFUNDS',
     'HOUST',
     'INDPRO',
     'MORTG',
     'PAYEMS',
     'PSAVERT',
     'TB3MS',
     'UMCSENT',
     'UNRATE',
     'GDP',
     'GDPC1',
     'GDPDEF',
     'M2V',
     'PCECC96',
     'GFDEBTN',
     'STLFSI',
     'M1',
     'M2',
     'PAYEMS',
    ]

    dflist = []
    for ser in fred_series_list:
        df = pd.DataFrame(fred.get_series(ser), columns=['value'])
        df.index.name='date'
        df.reset_index(inplace=True)
        df['field'] = ser
        dflist.append(df)

    df = pd.concat(dflist, ignore_index=True)

    if not 'fred' in con.table_names():
        df.to_sql('fred', con, index=False)

    else:
        current_list = pd.read_sql("SELECT field, MAX(date) as 'max_date' from fred group by field", con)
        dfc = df.merge(current_list, on=['field'])
        dfc = dfc[dfc['date']>dfc['max_date']].drop('max_date', axis=1)
        if not dfc.empty:
            dfc.to_sql('fred', con, index=False, if_exists='append')
			
			
if __name__ == '__main__':
	update_fred()
