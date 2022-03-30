import os
import numpy as np
import pandas as pd
import mysql.connector


def csv_files():

    # get names of only csv files
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".csv"):
            csv_files.append(file)

    return csv_files

def create_df(csv_files):
    data_path = os.getcwd()+'/'

    # loop through the files and create the dataframe
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path+file)
        except UnicodeDecodeError:
            # if utf-8 encoding error
            df[file] = pd.read_csv(data_path+file, encoding="ISO-8859-1")
        print(file)

    return df


def clean_tbl_name(filename):

    # rename csv, force lower case, no spaces, no dashes
    clean_tbl_name = filename.lower().replace(" ", "").replace(
        "-", "_").replace(r"/", "_").replace("\\", "_").replace("$", "").replace("%", "")

    tbl_name = '{0}'.format(clean_tbl_name.split('.')[0])

    return tbl_name


def clean_colname(dataframe):

    # force column names to be lower case, no spaces, no dashes
    dataframe.columns = [x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_").replace(
        "\\", "_").replace(".", "_").replace("$", "").replace("%", "") for x in dataframe.columns]

    # processing data
    replacements = {
        'timedelta64[ns]': 'varchar(100)',
        'object': 'varchar(100)',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(
        dataframe.columns, dataframe.dtypes.replace(replacements)))

    return col_str, dataframe.columns


def upload_to_db(host, database, user, password, tbl_name, col_str, file, dataframe, dataframe_columns):

    conn_string = "host=%s, database=%s, user=%s, password=%s" % (
        host, database, user, password)
    # conn_string = "host=localhost,user=alee,password=reallyisexcellent,database=nba_data"
    print("string is: " + conn_string)
    conn = mysql.connector.connect(
        host=host, database=database, user=user, password=password)
    cursor = conn.cursor()
    print('opened database successfully')

    print("drop table if exists %s;" % (tbl_name))
    print("create table %s (%s);" % (tbl_name, col_str))

    # drop table with same name
    cursor.execute("drop table if exists %s;" % (tbl_name))

    # create table
    cursor.execute("create table %s (%s);" % (tbl_name, col_str))
    print('{0} was created successfully'.format(tbl_name))

    # save df to csv
    dataframe.to_csv(file, header=dataframe_columns,
                     index=False, encoding='utf-8')

    col_names = col_str.replace(
        ' varchar(100)', '').replace(' int', '').replace(' float', '')

    # upload to db
    SQL_STATEMENT = """
    LOAD DATA INFILE '%s' INTO TABLE %s
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 LINES
    (%s);
    """ % (os.getcwd().replace(os.sep, '/') + '/' + file, tbl_name, col_names)

    print(SQL_STATEMENT)

    cursor.execute(SQL_STATEMENT)

    print('file copied to db')

    cursor.execute("grant select on table %s to public" % tbl_name)
    conn.commit()
    cursor.close()
    print('table {0} imported to db completed'.format(tbl_name))

    return
