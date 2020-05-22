import re
import sys

import sqlite3
from sqlite3 import Error, Warning, OperationalError

from sqlalchemy import create_engine
import pandas as pd

SQLDEFAULT = {
    'host': 'localhost',
    'port': 3306,
    'username': 'username',
    'password': 'password',
    'database': 'database',
}

def combine_old_and_new(data, current_img):
    
    comb = data.merge(
        current_img, 
        left_index=True,
        right_index=True,
        how='outer',
        suffixes=('','_drop')
    )
    
    if 'shares_drop' in comb.columns:
        comb['shares'] = comb['shares'].fillna(0)-comb['shares_drop'].fillna(0)

    comb = comb[comb['shares']!=0]
    if 'shares_drop' in comb.columns: comb.drop('shares_drop',axis=1, inplace=True)
    return comb

def create_file_connection(db_file):
    """ 
    create a database connection to a SQLite database 
    
    Example
    -------
    create_connection(db_file = r"C:\sqlite\db\pythonsqlite.db")
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
def _choose_driver():
    '''
    Returns
    -------
    Default driver based on platform
    '''
    platform = sys.platform
        
    if 'linux' in platform:
        return 'DRIVER=FreeTDS64;port=1433'
    elif 'win' in platform:
        return 'DRIVER={SQL SERVER}'
    else:
        assert False, 'Did not find driver'

def connect_to_default(
    database=None, 
    driver=None, 
    host=None,
    username=None,
    password = None,
    port = None,
    fp=None,
):
    '''
    Parameters
    ----------
    database : str
        SQL database you want to connect
    driver : str
        SQL driver you are connecting to
    host : None, str
        SQL server, if None use default
    username : None, str
        SQL username, if None use default
    password : None, str
        SQL password, if None use default
    fp : None, str
        If provided, connect to file instead
    
    Returns
    -------
    SQL alchemy engine
    '''
    if fp != None:
        return create_engine('sqlite:///{db}'.format(db=fp))
    
    if driver == None:
        driver = _choose_driver()
    elif 'driver' in SQLDEFAULT:
        driver = SQLDEFAULT['driver']
    
    if database is None: database = SQLDEFAULT['database']
    if host is None: host = SQLDEFAULT['host']
    if username is None: username = SQLDEFAULT['username']
    if password is None: password = SQLDEFAULT['password']    
    if port is None: port = SQLDEFAULT['port']
        
    con = create_engine(
        'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(
        host=host,
        port=port,
        user=username,
        password=password,
        db = database,
    ))
    
    return con

def retrieve_table_info(table_name, engine):
    sql = '''
    SELECT 
        c.name 'Column Name',
        t.Name 'Data type',
        c.max_length 'Max Length',
        c.precision ,
        c.scale ,
        c.is_nullable,
        ISNULL(i.is_primary_key, 0) 'Primary Key'
    FROM    
        sys.columns c
    INNER JOIN 
        sys.types t ON c.user_type_id = t.user_type_id
    LEFT OUTER JOIN 
        sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    LEFT OUTER JOIN 
        sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE
        c.object_id = OBJECT_ID('{}')
    '''.format(table_name.replace("'","''"))

    table_info = pd.read_sql(sql, engine)
    return table_info

def delete_all_duplicates_in_database(engine, database):
    '''
    Parameters
    ----------
    engine : sql alchemy engine
    database : str
        String of database that you want to drop duplicates
    '''
    for table_name in engine.table_names():
    
        sql = '''
        SELECT TOP 1 * FROM {}.{}
        '''.format(database, table_name)

        column_list = pd.read_sql(sql, engine).columns.tolist()

        str_columns = ','.join(['['+col+']' for col in column_list])

        sql = '''
        WITH CTE AS(
           SELECT *,
               RN = ROW_NUMBER()OVER(PARTITION BY {} ORDER BY [{}])
           FROM {}.{}
        )
        DELETE FROM CTE WHERE RN > 1
        '''.format(str_columns, column_list[0], database, table_name)

        engine.execute(sql)


def to_sql(engine, table_name, df, index=False, if_exists='append', force_add_columns=True):
    '''
    Parameters
    ----------
    engine : sql alchemy engine
    table_name : str
        Name of the table you want to create/store
        information
    df : pandas dataframe
    index : bool
        Whether to include the index
    if_exists : str
        {fail, replace, append}, default fail
        How to behave if the table already exists
        ------------------------------------------
        fail: Raise a ValueError.
        replace: Drop the table before inserting new values.
        append: Insert new values to the existing table.
    force_add_columns : bool
        Whether to add a column if it is in df but not in
        the table, table_name    
        
    Returns
    -------
    None
    '''

        
    if df.empty:
        return None
    
    if not engine.has_table(table_name):
        # create the table
        count_df = df.describe().loc['count']
        sql_str = ''
        for k, v in dict(df.dtypes).items():
            sql_dtype = None
            
            if k in count_df:
                number_non_null = count_df.loc[k]
                if number_non_null==0.0:
                    sql_dtype = 'varchar(max)'.upper()
                    
            
            if sql_dtype is None: 
                sql_dtype = re.sub("\d", "", v.name.replace('object', 'varchar(max)')).upper()
            
            if 'DATETIME[NS]' in sql_dtype:
                sql_dtype = sql_dtype.replace('DATETIME[NS]', 'DATE')
            
            sql_str += "[{}] {},\n".format(k, sql_dtype)


        sql_command = 'CREATE TABLE {} ({})'.format(table_name, sql_str)
        #print(sql_command)
        
        if engine.url.drivername == 'sqlite':
            
            database_url = engine.url.database
            conn = sqlite3.connect(database_url)
            c = conn.cursor()

            dtypes = [
                ('INT','INTEGER'),
                ('VARCHAR(MAX)', 'TEXT'),
                ('FLOAT','REAL'),
                ('\n',''),
            ]

            for old_dtype, new_dtype in dtypes:
                sql_command = sql_command.replace(old_dtype, new_dtype)
            
            sql_command = sql_command.replace(',)', ')')
            
            print(sql_command)
            c.execute(sql_command)
            
        else:
            print(sql_command)
            engine.execute(sql_command)
            
    try:
        df.to_sql(name=table_name, con=engine, index=index, if_exists=if_exists)
    except Exception as e:
        dtype_df = pd.DataFrame(df.dtypes, columns=['new Data type'])
        table_info = retrieve_table_info(table_name, engine)
        table_info.set_index('Column Name', inplace=True)

        table_info.index = [str(i).lower() for i in table_info.index.tolist()]
        dtype_df.index = [str(i).lower() for i in dtype_df.index.tolist()]
        
        agg_df = table_info.merge(dtype_df, left_index=True, right_index=True, how='outer')
        add_columns = list(set(agg_df[agg_df['Data type'].fillna('ADDTOTBL')=='ADDTOTBL'].index.tolist()))

        if len(add_columns) > 0 and force_add_columns:
            # add columns if they are note there
            new_add_col = []
            for col in add_columns:
                for jcol in df.columns:
                    if col.lower()==jcol.lower():
                        new_add_col.append(jcol) 
                
            count_df = df[new_add_col].describe().loc['count']
            
            for col, agg_col in zip(new_add_col, add_columns):
                v = agg_df.loc[agg_col]['new Data type']
                sql_dtype = None
                if col in count_df:
                    number_non_null = count_df.loc[col]
                    if number_non_null==0.0:
                        sql_dtype = 'varchar(max)'.upper()
                        
                if sql_dtype is None: sql_dtype = re.sub("\d", "", v.name.replace('object', 'varchar(max)')).upper()
                sql_str = "[{}] {}".format(col, sql_dtype)
                sql_command = 'ALTER TABLE [dbo].[{}] ADD {}'.format(table_name, sql_str)
                print('adding column: {} to table: {}'.format(col, table_name))
                engine.execute(sql_command)

            print('Attempting second storage after adding columns to: {}'.format(table_name))
            df.to_sql(name=table_name, con=engine, index=index, if_exists=if_exists)
            print('SUCCESS: second storage after adding columns to: {}'.format(table_name))
            return None

        print('Force Error')
        assert False, e
