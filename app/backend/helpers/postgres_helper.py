import sys, re, os
#######################################
repoName = 'checkup-for-coop'
pattern = rf'(.*{re.escape(repoName)})'
match = re.search(pattern, __file__)
baseDir = match.group(1)
sys.path.append(baseDir)
#######################################
import json, requests
import time
from time import sleep
from datetime import datetime
import psycopg2
import pandas as pd
import numpy as np
import io, csv, gc
#######################################
from dotenv import load_dotenv

env_path = os.path.join(baseDir, '.env')
load_dotenv(dotenv_path=env_path, override=True)
#######################################


def __create_connection():
    '''
    Dùng để tạo ra connection 
    '''
    _host = os.getenv('DB_HOST')
    _port = os.getenv('DB_PORT')
    _user = os.getenv('DB_USER')
    _pass = os.getenv('DB_PASSWORD')

    _db = os.getenv('DB_NAME')
    try:
        conn = psycopg2.connect(host=_host, port= _port, database=_db, user=_user, password=_pass)
        # Set search_path to include 'test' schema
        cur = conn.cursor()
        cur.execute("SET search_path TO test, public")
        conn.commit()
        cur.close()

    except Exception as e:
        print('issue in create connection:',e)
    else:
        # print(f'Connected to {self.database}')
        pass
    
    return conn

def __insert_list(data: list, schema, table, columns: list = None, external_connection = None, batch = 50000):
    '''
    Function này sẽ dùng lệnh `copy` để insert 1 nested list vào database
    
    Argument:

    `data`: nested list

    example: data = [
        [1,2,3],
        [4,5,6]
    ]

    `schema`: tên schema

    `table`: tên table

    `columns`: danh sách các columns tương ứng dựa trên data input
        > None: function sẽ insert data theo data input\n
        > list: function sẽ insert data theo thứ tự danh sách columns truyền vào
    
    `external_connection`: 
        > None (default): function sẽ insert data và commit ngay sau đó\n
        > connection: function sẽ insert data nhưng KHÔNG commit, với mục đích\n
        tiếp tục thực hiện các câu query kế tiếp cho đến khi người dùng commit 
    '''

    print("Length data:", len(data))
    # batch = 50000
    rows = 0
    if len(data) > 0:            
        if external_connection is None:
            conn = __create_connection()
        else: 
            conn = external_connection

        cursor = conn.cursor()
        if columns is None or len(columns) == 0:
            columnString = ""
        else:
            columnString = f"({','.join(columns)})"

        try:
            for i in range(0, len(data), batch):
                subData = data[i:i+batch]
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(subData)
                output.seek(0)
                sql = f"copy {schema}.{table} {columnString} from stdin with delimiter ',' csv"
                cursor.copy_expert(sql,output)
                rows += cursor.rowcount
                print(f'[{schema}.{table}] Inserted {rows} rows.')
                del output
                gc.collect()
            cursor.close()
            if external_connection is None:
                conn.commit()
                conn.close()
        except Exception as e:
            print(e)
            conn.rollback()
            cursor.close()
            if external_connection is None:
                conn.close()
            raise
        
    return rows

def query_to_df(query):
    """ Query ra một dataframe """
    connection = __create_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    df = pd.DataFrame(cursor, columns=[x[0] for x in cursor.description])
    cursor.close()
    connection.close()
    return df

def execute_sql(sql: str, ref_data: list = None, mode=None):
    """
    thực thi một câu lệnh SQL

    case mode:
    
    - 'select': hàm trả về list data

    - ['insert','delete','update']: hàm trả về số dòng đã thay đổi sau khi thực thi câu lệnh

    - others: like `proc` , `truncate` , `drop` , `create` , `grant` -> hàm trả về None

    """

    connection = __create_connection()
    cursor = connection.cursor()

    if mode == 'select':
        cursor.execute(sql)
        data =  cursor.fetchall()
        cursor.close()
        connection.close()
        return data
    
    elif mode in ['insert','delete','update']:
        try:
            if ref_data is not None:
                if len(ref_data) > 0:
                    if isinstance(ref_data[0], list) or isinstance(ref_data[0], tuple):
                        cursor.executemany(sql, ref_data)
                    else:
                        cursor.execute(sql, ref_data)
            else:
                cursor.execute(sql)
            connection.commit()
            rows = cursor.rowcount
            cursor.close()
            connection.close()
            return rows
        except Exception as e:
            print(e)
            connection.rollback()
            cursor.close()
            connection.close()
            raise

    else:
        try:
            cursor.execute(sql)
            connection.commit()
            cursor.close()
            connection.close()
            return
        except Exception as e:
            connection.rollback()
            cursor.close()
            connection.close()
            raise Exception(e)
        
def upsert_dataframe(data, schema, table, key_columns: list, batch = 10000, columns = None, do_nothing = False):
    """
    Function upsert data theo key_columns
    
    `data`: list, dataframe or dictionary
    
    Nếu `data` có kiểu list: Phải chỉ định thứ tự cột qua biến `columns`
    """

    if isinstance(data, pd.DataFrame):
        rows = data.values.tolist()
        columns = data.columns
    
    elif isinstance(data, list):
        if isinstance(data[0], dict):
            df = pd.DataFrame(data)
            rows = df.values.tolist()
            columns = df.columns
        else:
            rows = data.copy()

    if len(key_columns) == 0: # only insert
        changes = __insert_list(rows, schema=schema, table=table, columns= list(columns))
        return changes 
    
    else:
        originColumns = ", ".join(columns) # (col1, col2, col3)
        if do_nothing == False:
            excludes = []
            for col in columns:
                if col not in key_columns:
                    string = f'{col} = EXCLUDED.{col}'
                    excludes.append(string)
            excludedColumns = ",\n\t".join(excludes) # Nếu trùng keys thì chỉ update các columns này col1 = EXCLUDED.co1...
            action = f"DO UPDATE SET\n\t{excludedColumns}"
        else:
            action = f"DO NOTHING"

        conn = __create_connection()
        cur = conn.cursor()

        tmpTable = f'tmp_{table}'
        # create temp table
        sql = f"create temporary table if not exists {tmpTable} as select * from {schema}.{table} where 1=2"
        cur.execute(sql)

        upsertSql = f"""
            INSERT INTO {schema}.{table}
            select * from {tmpTable}
            ON CONFLICT ({', '.join(key_columns)})
            {action}
        """

        changes = 0
        for i in range(0, len(rows), batch):
            try:
                batchData = rows[i:i+batch]

                # insert to temp table
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(batchData)
                output.seek(0)
                sql = f"copy {tmpTable} ({originColumns}) from stdin with delimiter ',' csv"
                cur.copy_expert(sql, output)

                # upsert to master
                cur.execute(upsertSql)
                print(f"\t=====> {schema}.{table} ({cur.rowcount} rows)")
                changes += cur.rowcount
                
                # truncate table temp
                sql = f"truncate table {tmpTable}"
                cur.execute(sql)

            except Exception as e:
                print(e)
                conn.rollback()
                cur.close()
                conn.close()
                raise

        conn.commit()
        print('Done')
        cur.close()
        conn.close()

        return changes

def insert_dataframe(df: pd.DataFrame, schema, table, columns: list = []):

    if len(df) == 0:
        print('Do nothing in insert_dataframe')
        return
    
    originColumns = ", ".join(df.columns) # (col1, col2, col3)
    
    rows = df.values.tolist()

    conn = __create_connection()
    cur = conn.cursor()

    tmpTable = f'tmp_{table}'
    # create temp table
    sql = f"create temporary table if not exists {tmpTable} as select * from {schema}.{table} where 1=2"
    cur.execute(sql)

    changes = 0
    batch= 10000
    for i in range(0, len(rows), batch):
        try:
            batchData = rows[i:i+batch]

            # insert to temp table
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerows(batchData)
            output.seek(0)
            sql = f"copy {tmpTable} ({originColumns}) from stdin with delimiter ',' csv"
            cur.copy_expert(sql, output)

        except Exception as e:
            print(e)
            conn.rollback()
            cur.close()
            conn.close()
            raise
    
    if len(columns) > 0:
        conditions = []
        for col in columns:
            condition = f'a.{col} = b.{col}'
            conditions.append(condition)

        sql = f"""
            DELETE FROM {schema}.{table} a
            USING {tmpTable} b
            WHERE true AND
            {" AND ".join(conditions)}
        """
        try:
            cur.execute(sql)
        except Exception as e:
            conn.rollback()
            conn.close()
            raise Exception(e)
        
        print(f'Done delete {cur.rowcount} row(s)')

    sql = f"""
        INSERT INTO {schema}.{table} ({originColumns})
        SELECT {originColumns}
        FROM {tmpTable} 
    """
    try:
        cur.execute(sql)
    except Exception as e:
        conn.rollback()
        conn.close()
        raise Exception(e)
    
    print(f'Done insert {cur.rowcount} row(s)')

    conn.commit()
    cur.close()
    conn.close()

    return changes

def transform_dataframe(df: pd.DataFrame, tableName: str, schema: str, matched_columns = False, number_seperate_by = ','):
    '''
        Transform df gốc (dtype = str) thành kiểu dữ liệu tương ứng với table truyền vào

        Arguments:

        `df`: dataframe

        `tableName`: table name

        `schema`: schema
    '''
    if len(df) > 0:
        connection = __create_connection()
        cursor = connection.cursor()
        try:
            transformDF = df.copy()
            columns = transformDF.columns
            transformDF.columns = transformDF.columns.str.lower()

            # postgres
            if os.getenv('DB_NAME').lower().startswith("postgre"):
                sql = f"""
                    select column_name, data_type
                    from information_schema.columns
                    where table_schema = '{schema.lower()}'
                    and table_name = '{tableName.lower()}'
                    and column_name in {tuple([str(x).lower() for x in columns])}
                """
                cursor.execute(sql)
                metadata = cursor.fetchall()

                if matched_columns:
                    print('Get matched columns')
                    db_columns = [x[0] for x in metadata]
                    transformDF = transformDF[db_columns]

                if len(metadata) > 0:
                    for row in metadata:
                        columnName = row[0]                        
                        if ("char" in str(row[1]).lower() or "text" in str(row[1]).lower()):
                            transformDF[columnName] = transformDF[columnName].astype(str)

                        for type in ["date", "time", "timestamp"]:
                            if type in str(row[1]).lower():
                                transformDF[columnName] = pd.to_datetime(transformDF[columnName],errors="coerce")
                                if "time zone" in str(row[1]).lower():
                                    transformDF[columnName] = transformDF[columnName].dt.strftime("%Y-%m-%d %H:%M:%S%z")
                                else:
                                    transformDF[columnName] = transformDF[columnName].dt.strftime("%Y-%m-%d %H:%M:%S")
                                transformDF[columnName] = transformDF[columnName].astype(str)
                                transformDF[columnName] = transformDF[columnName].replace({"nan": None})
                                transformDF[columnName] = transformDF[columnName].replace({"NaT": None})

                        for type in ["num", "float", "int"]:       
                            if type in str(row[1]).lower():
                                transformDF[columnName] = transformDF[columnName].astype(str).replace({"nan": None, "null": None, "None": None})
                                
                                transformDF[columnName] = transformDF[columnName].astype(str).str.replace(number_seperate_by, "")

                                if number_seperate_by == ".":
                                    transformDF[columnName] = transformDF[columnName].astype(str).str.replace(',', ".")              
                                
                                transformDF[columnName] = pd.to_numeric(transformDF[columnName], errors= 'coerce')

                                if 'int' in str(row[1]).lower():
                                    def format_number(x):
                                        if x.is_integer():
                                            return int(x)
                                        else:
                                            return x
                                    try:
                                        transformDF[columnName] = transformDF[columnName].apply(format_number)
                                    except:
                                        pass
                                    transformDF[columnName] = pd.to_numeric(transformDF[columnName], errors= 'coerce')
                                    transformDF[columnName]= transformDF[columnName].astype('Int64')
                                
                                break

                    transformDF = transformDF.replace(
                        {np.nan: None, "nan": None, "NaN": None, "null": None, "None": None}
                    )
                    print(f"Transform {tableName.lower()} done")

                else:
                    print('Table or column is not matched with DB.')

            cursor.close()
            connection.close()
            
            return transformDF
    
        except Exception as e:
            print("issue transform_dataframe:", e)
            cursor.close()
            connection.close()
            raise
    else:
        print("Nothing to transform")
        return df