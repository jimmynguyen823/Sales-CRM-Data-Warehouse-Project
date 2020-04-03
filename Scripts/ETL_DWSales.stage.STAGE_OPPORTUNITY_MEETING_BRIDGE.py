import os
from typing import Optional
import pandas as pd
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.sql import text
import ibm_db_sa
#import pyodbc
#import jaydebeapi

#Database URL = jdbc:db2://rodpdb.chq.ei:50000/RO_CENT
#pip install selenium -t "C:\Program Files (x86)\Python 3.5\Lib\site-packages"


'''
def write_to_sql_server(                                                                                # create function that writes to SQL Server
    table_data: pd.DataFrame,
    table: str,
    server: str,
    database: str,
    driver: str,
    schema: Optional[str] = None,
    if_exists: str = "append",
):
    con = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{{{database}}}?driver={driver}?trusted_connection=yes")

    con.execute('DELETE FROM stage.STAGE_OPPORTUNITY_SERVICE_ITEM; DBCC CHECKIDENT ("stage.STAGE_OPPORTUNITY_SERVICE_ITEM", RESEED, 0)')
    table_data.to_sql(table, con=con, schema=schema, if_exists=if_exists, index=False)
'''

def read_from_db2(username: str, password: str, server: str, port: str, database: str):                  # create function that takes in 5 parameters 
    con = sqlalchemy.create_engine(f"ibm_db_sa://{username}:{password}@{server}:{port}/{database}")      # create connection variable to DB2 database using the parameters
    df = pd.read_sql(                                                                                  # return sql query output
    """                                                                                 
SELECT 
        OPPORTUNITY_ID
        ,MEETING_ID
FROM sales.opportunity_meeting_link            
    """ 
     , con)
    
    df.columns = map(lambda x: str(x).upper(), df.columns)
    myfile = r'F:\FTP\DWSales\stage_opportunitymeetingbridge.csv'
    
    if os.path.isfile(myfile):
        os.remove(myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_opportunitymeetingbridge.csv', index=None, header=True) #outputs csv file to a file directory
    else:    ## Show an error ##
        print("Error: %s file not found" % myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_opportunitymeetingbridge.csv', index=None, header=True) #outputs csv file to a file directory                                                       
    





if __name__ == "__main__":
    df = read_from_db2(
        username="powerbi",
        password="BBbig@biz123",
        server="vizcrm.chq.ei",
        port="50000",
        database="vizcrm",
    )
'''
    write_to_sql_server(
        table_data=df,
        table="STAGE_OPPORTUNITY_SERVICE_ITEM",
        server="GBSG2M0001",
        database="DWSales",
        driver="SQL+Server+Native+Client+11.0",
        schema="stage",
        if_exists = "append"
    )
'''

