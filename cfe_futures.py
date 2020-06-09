import os
import sys
import argparse

#fp = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#sys.path.insert(0, fp)

from time import sleep
import pandas as pd
import datetime as dt

from _selenium_util import get_driver, selenium_wait
from selenium import webdriver
from _sql_util import connect_to_default, to_sql

def get_finished_list(engine):
    try:
        urls = pd.read_sql_table('cboe_urls', engine)
        return urls.unstack().tolist()
    except:
        print('error cboe_urls')
        return []

def pull_link_data(l):
    data = pd.read_csv(l)
    data['Expiration'] = os.path.basename(l)
    data['Product'] = os.path.basename(os.path.dirname(l))
    return data

def update_data(engine, data):
    '''
    update data in cboe database
    '''
    if 'cboe' in engine.table_names():
        sql = 'SELECT DISTINCT trade_date,expiration,product FROM cboe'
        existing_values = pd.read_sql(sql, engine)
    else:
        existing_values = pd.DataFrame(columns=['trade_date','expiration','product'])
        
    empty_data = data[
        ~((data['trade_date'].isin(existing_values['trade_date']))&\
        (data['expiration'].isin(existing_values['expiration']))&\
        (data['product'].isin(existing_values['product'])))
    ]

    empty_data.to_sql('cboe', engine, if_exists='append', index=False)

def get_cboe(engine=None):
    #driver = get_driver(hidden=True, ip=False)
    driver = webdriver.Chrome()
    links = get_links(driver)

    data_list = []
    
    if engine != None:
        stop_pull = get_finished_list(engine)
    else:
        stop_pull = []
        
    new_links = [l for l in links if l not in stop_pull]

    for l in new_links:
        try:
            data = pull_link_data(l)
            data_list.append(data)

            finished = pd.to_datetime(
                data['Trade Date'].max()
            )+dt.timedelta(5)<pd.datetime.today().date()

            if finished: stop_pull.append(l)

        except:
            continue

    data = pd.concat(data_list, ignore_index=True)
    data.columns = [col.replace(' ','_').lower() for col in data]
    stop_pull = pd.DataFrame(stop_pull, columns=['url'])
    
    return data, stop_pull
    
def update_cboe(db):
    '''
    main - update cboe database for prices
    '''
    engine = connect_to_default(db)
    data, stop_pull = get_cboe(engine)
    update_data(engine, data)
    if len(stop_pull)>0:
        stop_pull.to_sql('cboe_urls', engine, if_exists='append', index=False)

            
def get_links(driver):
    
    url = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/'

    driver.get(url)

    selector = driver.find_element_by_id('historical-data-select')

    links = []
    for item in selector.find_elements_by_tag_name('option'):
        item.click()
        selenium_wait(driver, ele_id='historical-data-table')
        while True:
            try:
                sleep(0.25)
                tbl = driver.find_element_by_id('historical-data-table')
                links_ele = tbl.find_elements_by_tag_name('a')
                new_links = [l.get_attribute('href') for l in links_ele]
                links.extend(new_links)
                break
            except:
                pass
                
    driver.close()

    return links

if __name__=='__main__':
    update_cboe(db='tiger')
