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
CLIENT_NUMBER, 
CLIENT_NAME, 
MASTER_CLIENT_NUMBER, 
MASTER_CLIENT_NAME, 
ADDR_LINE1, 
ADDR_LINE2, 
CITY, 
STATE, 
ZIP, 
COUNTRY, 
PHONE, 
--IRS_NO, 
--BASE_IRS_NO, 
--MASTER_CLIENT_SOURCE, 
--COMPANY_KEY, 
--COMPANY_NAME, 
--DIVISION1_KEY, 
--DIVISION1, 
--DIVISION2_KEY, 
--DIVISION2, 
--DIVISION3_KEY, 
--DIVISION3, 
--DIVISION4_KEY, 
--DIVISION4, 
--TRACE_VALUE, 
BRANCH_CLIENT_INDICATOR, 
ACCOUNT_TYPE, 
--INSERT_DATE, 
--UPDATE_DATE, 
--DATA_MART_KEY, 
--MAPPING_KEY, 
GCI, 
GCI_NAME, 
GCI_ADDR_LINE1, 
GCI_ADDR_LINE2, 
GCI_CITY, 
GCI_STATE, 
GCI_ZIP, 
GCI_COUNTRY, 
GCI_PHONE, 
GCI_FLAG, 
BCC_REPLACEMENT_FLAG, 
MASTER_CLIENT_REPORTING, 
FAMILY_TREE_APEX_GCI, 
FAMILY_TREE_APEX_NAME, 
FAMILY_TREE_APEX_UNIQUE_YN, 
FAMILY_TREE_PARENT_GCI, 
FAMILY_TREE_PARENT_NAME, 
FAMILY_TREE_HAS_PARENT_YN, 
CLIENT_ACTIVE_YN, 
--AGENT_STATUS_CODE, 
--BILL_STATUS_CODE, 
--BRANCH_STATUS_CODE, 
--CONSIGNEE_STATUS_CODE, 
--CUSTOMER_STATUS_CODE, 
--CUST_CRMACCT_STATUS_CODE, 
--CUST_VENDOR_STATUS_CODE, 
--EDIPARTNER_STATUS_CODE, 
--EXPORTER_STATUS_CODE, 
--EXPO_VIZCLI_STATUS_CODE, 
--IMPORTER_STATUS_CODE, 
--PAYEE_STATUS_CODE, 
--SERVICEPRO_STATUS_CODE, 
--SVCPRO_CRMACCT_STATUS_CODE, 
--SHIPPER_STATUS_CODE, 
--USAGE_DISTINCTION_BASE_GCI, 
--USAGE_DISTINCTION_TYPE, 
CUSTOMER_ORGANIZATION_ID, 
CUSTOMER_ORGANIZATION_NAME, 
DUNS_NUMBER

FROM dimensions.new_client_dimension 
WHERE client_active_yn = 'Y'
--AND CUSTOMER_ORGANIZATION_ID = 'C0008917'                   
    """ 
     , con)
    
    df.columns = map(lambda x: str(x).upper(), df.columns)
    myfile = r'F:\FTP\DWSales\stage_clientdimension.csv'
    
    if os.path.isfile(myfile):
        os.remove(myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_clientdimension.csv', index=None, header=True) #outputs csv file to a file directory
    else:    ## Show an error ##
        print("Error: %s file not found" % myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_clientdimension.csv', index=None, header=True) #outputs csv file to a file directory                                                       
    





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

