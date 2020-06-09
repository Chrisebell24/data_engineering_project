import os
import sys
import pandas as pd
import numpy as np

#fp = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#sys.path.insert(0, fp)

from _sql_util import connect_to_default

import warnings
warnings.filterwarnings('ignore')

def main():
    fp = os.path.dirname(os.path.abspath(__file__))
    fp_listdir = os.listdir(fp)
    files = [os.path.join(fp, i) for i in fp_listdir]
    con = connect_to_default('tiger')
    
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
    
    settings = pd.read_sql("SELECT fp,val from routine_settings where field='active'", con, index_col='fp')
    settings['val'] = np.where(settings['val'].astype(str) =='1',True, np.where(settings['val'].astype(str)=='0', False, settings['val']))
    settings = settings.to_dict()['val']

    report = []
    for f in files:
        f_ = os.path.basename(f)
        active = settings.get(f_.replace('.py',''))

        if active and '.py' in f and 'config' not in f_ and os.path.basename(__file__) not in f_:

            command = "python {}".format(f)
            print('running: {}'.format(command))
            try:
                os.system(command)
            except Exception as e:
                report.append((f_, e, pd.to_datetime.today().strftime('%Y-%m-%d %H:%M:%S') ))
    
    if len(report)>0:
        report = pd.DataFrame(report,columns=['routine', 'error_msg', 'date'])
        report.to_sql('routines_report', con, if_exists='append',index=False)
    
    if 'linux' in sys.platform:
    
        print('backing up sql databases')
        
        current_date = pd.datetime.today().strftime('%Y%m%d')
        backup_dir = os.path.join( os.environ['HOME'], 'backup')
        if not os.path.exists( backup_dir ):
            os.makedirs(backup_dir)
            
        backup_fp = os.path.join( backup_dir,  '{}.sql.gz'.format(current_date) )
        cmd = 'podman exec db /usr/bin/mysqldump -u reader --password=redaer --all-databases | gzip -9 > '+backup_fp
        os.system(cmd)
        
        print('pruning backup files')
        
        backup_files = [os.path.join(backup_dir, i) for i in os.listdir(backup_dir)]
        
        df = pd.DataFrame(backup_files, columns=['fp'])

        df['date'] = df['fp'].apply(lambda x: pd.to_datetime(os.path.basename(x).split('_')[0].split('.')[0] ))
        df['days'] = (pd.datetime.today() - df['date']).dt.days
        df['last_date'] = df.groupby([df['date'].dt.year, df['date'].dt.month])['date'].transform('last')==df['date']
        df['delete'] = (df['days']>365)|((df['last_date']==False)&(df['days']>30))

        for fp in df[df['delete']]['fp']:
            os.remove(fp)
            
if __name__ == '__main__':
    main()
