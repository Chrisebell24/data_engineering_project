import requests
import pandas as pd
from bs4 import BeautifulSoup
from lxml.html import fromstring

import os
import platform
import numpy as np
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

class ProxyRequests:
    
    def __init__(self):
        self._proxies = get_proxies()
        
    def get(self, url):
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        
        while len(self._proxies) > 0:
            try:
                req = requests.get(
                    url, 
                    headers=headers,
                    proxies={'http': self._proxies[0],'https': self._proxies[0]}
                )
                
                return req
            except:
                self._proxies.pop(0)
                

def get_proxies():
    '''
    Get list of proxies you can use
    '''
    req = requests.get(url = 'https://free-proxy-list.net/')
    soup = BeautifulSoup(req.text,'lxml')
    tbl = soup.find_all(id='proxylisttable')[0]
    data = [tuple([td.get_text() for td in tr.find_all('td')]) for tr in tbl.find_all('tr')]
    p = pd.DataFrame(data).dropna().iloc[:,0:2]
    res = (p[0]+':'+p[1]).tolist()
    return res



def selenium_wait(browser, ele_id, delay=3):

    try:
        myElem = WebDriverWait(browser, delay).until(EC.presence_of_element_located((By.ID, ele_id)))
    except TimeoutException:
        pass
    

def get_driver(hidden=False, printout=False, executable_path=None, ip=True):
    '''
    Parameters
    ----------
    hidden : bool
        shown or not
    printout : str
        show paths
    executable_path : str
        override path of driver
    ip : bool
        Use random IP (only for hidden=False and chrome)
    
    Returns
    -------
    selenium web driver
    '''
    
    fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'selenium_drivers')
    
    if printout: print(fp)
        
    full_fp = [os.path.join(fp, i) for i in os.listdir(fp)]

    platform_name = platform.system().lower() 
    
    if printout: print(platform_name)
    
    if 'windows' in platform_name:
        test_drivers = [i for i in full_fp if '_win' in i]
    elif 'linux' in platform_name:
        test_drivers = [i for i in full_fp if '_lin' in i]
    elif 'darwin' in platform_name:
        test_drivers = [i for i in full_fp if '_mac' in i]
    else:
        test_drivers = full_fp
    
    test_drivers.append('/usr/local/bin/chromedriver')
    
    iid=0
    while iid < len(test_drivers):
        curr_driver = test_drivers[iid]
            
        try:
            if not hidden:
                if 'chrome' in test_drivers[iid]:
                    
                    if printout: print(curr_driver)
                    
                    if ip:
                        options = webdriver.chrome.options.Options()
                        proxies = get_proxies()
                        proxy = np.random.choice(proxies)
                        print(proxy)
                        options.add_argument('--proxy-server=http://{proxy}'.format(proxy=proxy))
                        d = webdriver.Chrome(executable_path=curr_driver, options=options)
                    else:
                        d = webdriver.Chrome(executable_path=curr_driver)
                        
                    return d
            else:
                
                if 'phantom' in test_drivers[iid]:
                    if printout: print(curr_driver)
                    d = webdriver.PhantomJS(executable_path=curr_driver)
                    return d

        except Exception as e:
            if printout: print(e)
        
        iid+=1
        
    try:
        return webdriver.Chrome(executable_path=executable_path) 
    except Exception as e:
        if printout: print('2nd last failed')
        if printout: print(e)
        
        try:
            return webdriver.PhantomJS(executable_path=executable_path) 
        except Exception as e:
            if printout: print('last failed')
            if printout: print(e)