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

    con.execute('DELETE FROM stage.STAGE_MEETING; DBCC CHECKIDENT ("stage.STAGE_MEETING", RESEED, 0)')
    table_data.to_sql(table, con=con, schema=schema, if_exists=if_exists, index=False)
'''

def read_from_db2(username: str, password: str, server: str, port: str, database: str):                  # create function that takes in 5 parameters 
    con = sqlalchemy.create_engine(f"ibm_db_sa://{username}:{password}@{server}:{port}/{database}")      # create connection variable to DB2 database using the parameters
    df = pd.read_sql(                                                                                  # return sql query output
    """                                                                                 
SELECT 
--MEETING_KEY, 
m.MEETING_ID, 
--MEC_METADATA_ID_1, 
--MEC_METADATA_ID_2, 
m.ACCOUNT_ID, 
m.ACCOUNT_NAME, 
m.ACTIVITY_CONTEXT, 
m.ADDRESS_LINE1, 
m.ADDRESS_LINE2, 
m.ADDRESS_LINE3, 
m.CITY, 
m.COMPANY_ORGANIZATION_ID, 
m.COUNTRY_NAME, 
m.CREATION_DATE_TIME, 
m.GCI_NUMBER, 
m.INDUSTRY_CODE, 
m.INDUSTRY_DESCRIPTION, 
m.INVOLVED_BRANCHES, 
m.IS_REVIEW, 
m.LAST_MODIFICATION_DATE_TIME, 
m.LEGAL_NAME, LOCATION_NAME, 
m.MEETING_END_DATE_TIME, 
m.MEETING_METHOD_CODE, 
m.MEETING_OBJECTIVE, 
m.MEETING_START_DATE_TIME, 
m.MEETING_SUBJECT, 
m.MEETING_TYPE_CODE, 
mc.meeting_contact_roles as MEETING_CONTACT_ROLES,
mc.employee_id AS MEETING_CONTACT_ROLE_EMPLOYEE_ID,
m.OPPORTUNITY_COUNT, 
m.OWNER_EMPLOYEE_ID, 
m.OWNING_BRANCH_CODE, 
m.POSTAL_CODE, 
m.PRODUCTS_AIR_FLAG_YN, 
m.PRODUCTS_CUSTOMS_FLAG_YN, 
m.PRODUCTS_DISTRIBUTION_FLAG_YN, 
m.PRODUCTS_INSURANCE_FLAG_YN, 
m.PRODUCTS_OCEAN_FLAG_YN, 
m.PRODUCTS_ORDER_MANAGEMENT_FLAG_YN, 
m.PRODUCTS_OTHER_FLAG_YN, 
m.PRODUCTS_PROJECT_CARGO_FLAG_YN, 
m.PRODUCTS_TRANSCON_FLAG_YN, 
m.PRODUCT_CODES, 
m.STATE_PROVINCE_NAME, 
m.STATUS_CODE, 
m.VERTICAL_INDUSTRY_TEAM_CODE, 
--CHECKSUM, INSERT_DTTM, 
--UPDATE_DTTM, 
m.MEETING_PRIMARY_EXTERNAL_CONTACT_ID, 
m.DUNS_NUMBER, 
m.POST_MEETING_NOTES, 
m.PRE_MEETING_NOTES, 
m.CUSTOMER_ACCOUNT_OWNER_EMPLOYEE_ID, 
m.CLOSE_DATE_TIME
FROM sales.MEETING m
INNER JOIN sales.MEETING_CONTACTS mc
       ON mc.meeting_key = m.meeting_key
       AND mc.meeting_contact_roles IN ('OWNER', 'ATTENDEE')
WHERE m.MEETING_START_DATE_TIME >= CURRENT_DATE - 2 YEAR                      
    """ 
     , con)
    
    df.columns = map(lambda x: str(x).upper(), df.columns)
    myfile = r'F:\FTP\DWSales\stage_meetings.csv'
    
    if os.path.isfile(myfile):
        os.remove(myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_meetings.csv', index=None, header=True) #outputs csv file to a file directory
    else:    ## Show an error ##
        print("Error: %s file not found" % myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_meetings.csv', index=None, header=True) #outputs csv file to a file directory   





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
        table="STAGE_MEETING",
        server="GBSG2M0001",
        database="DWSales",
        driver="SQL+Server+Native+Client+11.0",
        schema="stage",
        if_exists = "append"
    )
'''
