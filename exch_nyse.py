from _exch_util_update import update_exch
import os
import pandas as pd
from ftplib import FTP
from tempfile import NamedTemporaryFile

def get_nyse():

    def customWriter(line):
        tmp_f.write(line + "\n")

    ftp_url = 'ftp.nasdaqtrader.com'

    with NamedTemporaryFile(delete=False) as f:
        ftp = FTP(ftp_url)
        print(ftp.login())
        print(ftp.cwd('/symboldirectory/'))
        fp = f.name
        with open(fp, mode='w') as tmp_f:
            ftp.retrlines('RETR bxtraded.txt', customWriter)

    df = pd.read_csv(fp, delimiter='|')

    os.remove(fp)
    df = df[~df['Symbol'].isnull()]
    return df

def main():
    df = get_nyse()
    update_exch(df=df, tbl='exch_nyse', ticker_col='Symbol')
    
if __name__=='__main__':
    main()