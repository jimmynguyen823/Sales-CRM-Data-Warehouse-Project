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
WITH opp AS (
SELECT 
opportunity.OPPORTUNITY_ID, 
--OC_METADATA_ID_1, 
--OC_METADATA_ID_2, 
opportunity.ACTIVITY_CONTEXT, 
--opportunity.COMPANY_ORGANIZATION_ID, 
opportunity.CREATION_DATE_TIME, 
opportunity.ESTIMATED_COMPLETION_DATE, 
opportunity.CLOSE_DATE_TIME,
opportunity.CLOSE_YEAR_MONTH, 
opportunity.LAST_MODIFICATION_DATE_TIME, 
opportunity.ESTIMATED_TOTAL_MONTHLY_REVENUE_FROM_SERVICE_ITEMS_AMOUNT, 
opportunity.ESTIMATED_TOTAL_MONTHLY_REVENUE_FROM_SERVICE_ITEMS_CURRENCY_CODE, 
opportunity.GCI_NUMBER, 
--INDUSTRY_CODE, 
opportunity.INVESTMENT_AND_DESIRABILITY_CODE, 
opportunity.IS_PROJECT_BASED, 
opportunity.IS_REQUEST_FOR_INFORMATION, 
opportunity.IS_REQUEST_FOR_PRICE, 
opportunity.IS_REQUEST_FOR_QUOTE, 
--LATEST_EXTERNAL_MEETING_DATE, 
opportunity.LEGAL_NAME, 
opportunity.OPPORTUNITY_DESCRIPTION, 
opportunity.OPPORTUNITY_OWNER_ID, 
owner.branch AS OPPORTUNITY_OWNER_BRANCH,
--opportunity.OPPORTUNITY_SOLUTION, 
opportunity.OPPORTUNITY_STATUS_CHANGE_DATE_TIME, 
opportunity.OPPORTUNITY_STATUS_CODE, 
opportunity.OPPORTUNITY_STEP_PROGRESSION_CODE, 
opportunity.OPPORTUNITY_STEP_PROGRESSION_CODE_CHANGE_DATE_TIME, 
opportunity.OPPORTUNITY_TYPE_CODE, 
opportunity.OPPORTUNITY_TYPE_CODE_CHANGE_DATE_TIME, 
opportunity.OWNING_BRANCH_CODE, 
opportunity.PERCENT_OF_ESTIMATED_MONTHLY_REVENUE_FROM_INCUMBENCY_CODE, 
opportunity.PRIMARY_FOCUS_CODE, 
--PROJECT_BASED_DURATION_IN_MONTHS, 
--REASON_LOST_CODE, 
opportunity.SEGMENTATION_CODE, 
opportunity.TRANSITION_STATUS_CODE, 
opportunity.USER_ESTIMATED_TOTAL_MONTHLY_REVENUE_AMOUNT, 
opportunity.USER_ESTIMATED_TOTAL_MONTHLY_REVENUE_AMOUNT_CURRENCY_CODE, 
--INSERT_DTTM, 
--UPDATE_DTTM, 
--CHECKSUM, 
opportunity.TRANSITION_STATUS_SET_TIME, 
opportunity.ACCOUNT_ID, 
opportunity.ACCOUNT_NAME, 
opportunity.CUSTOMER_ACCOUNT_MANAGEMENT_LEVEL_CODE, 
--opportunity.BRANCH_KEY, 
--opportunity.CLIENT_KEY, 
--opportunity.CREATION_DATE_TIME_DATE_KEY, 
--opportunity.ESTIMATED_COMPLETION_DATE_DATE_KEY, 
--opportunity.LAST_MODIFICATION_DATE_TIME_DATE_KEY, 
opportunity.ACTUAL_BID_KICK_OFF_DATE, 
opportunity.BID_RECEIVED_ACT_DATE, 
opportunity.BID_RECEIVED_EST_DATE, 
opportunity.BID_ROUND_ACTUAL_COMPLETION_DATE, 
opportunity.BID_ROUND_ESTIMATED_COMPLETION_DATE, 
opportunity.BID_ROUND_NUMBER, 
opportunity.CUSTOMER_DEBRIEF_ACTUAL_DATE, 
opportunity.CUSTOMER_DEBRIEF_ESTIMATED_DATE, 
opportunity.ESTIMATED_BID_KICK_OFF_DATE, 
opportunity.INVOLVED_BRANCHES, 
opportunity.OUTCOME_RECEIVED_ACTUAL_DATE, 
opportunity.OUTCOME_RECEIVED_ESTIMATED_DATE, 
opportunity.PRODUCTS_AIR_FLAG_YN, 
opportunity.PRODUCTS_CUSTOMS_FLAG_YN, 
opportunity.PRODUCTS_DISTRIBUTION_FLAG_YN, 
opportunity.PRODUCTS_OCEAN_FLAG_YN, 
opportunity.PRODUCTS_ORDER_MANAGEMENT_FLAG_YN, 
opportunity.PRODUCTS_OTHER_FLAG_YN, 
opportunity.PRODUCTS_PROJECT_CARGO_FLAG_YN, 
opportunity.PRODUCTS_TRANSCON_FLAG_YN, 
opportunity.PRODUCT_CODES_FROM_BID, 
opportunity.INDUSTRY_DESCRIPTION, 
opportunity.REASON_LOST_DESCRIPTION, 
opportunity.VERTICAL_INDUSTRY_TEAM_CODE, 
opportunity.ACCOUNT_STATUS, 
opportunity.CUSTOMER_ACCOUNT_OWNER_EMPLOYEE_ID, 
opportunity.CUSTOMER_ACCOUNT_OWNER_NAME, 
opportunity.EXTERNAL_CONTACT_NAMES, 
opportunity.DUNS_NUMBER, 
opportunity.PRODUCTS_INSURANCE_FLAG_YN, 
opportunity.PRODUCT_CODES_FROM_USER, 
opportunity.PRODUCTS_BID_AIR_FLAG_YN, 
opportunity.PRODUCTS_BID_CUSTOMS_FLAG_YN, 
opportunity.PRODUCTS_BID_DISTRIBUTION_FLAG_YN, 
opportunity.PRODUCTS_BID_INSURANCE_FLAG_YN, 
opportunity.PRODUCTS_BID_OCEAN_FLAG_YN, 
opportunity.PRODUCTS_BID_ORDER_MANAGEMENT_FLAG_YN, 
opportunity.PRODUCTS_BID_OTHER_FLAG_YN, 
opportunity.PRODUCTS_BID_PROJECT_CARGO_FLAG_YN, 
opportunity.PRODUCTS_BID_TRANSCON_FLAG_YN, 
opportunity.STEP_PROGRESSION_CODE_20_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_50_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_70_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_90_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_100_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_20_DAYS_ON_HOLD,
COUNT(DISTINCT link.meeting_id) AS OPPORTUNITY_MEETING_COUNT,
MIN(DATE(compmtg.meeting_start_date_time)) AS OPPORTUNITY_FIRST_COMPLETED_MTG_START_DATE,
MAX(DATE(compmtg.meeting_start_date_time)) AS OPPORTUNITY_LAST_COMPLETED_MTG_START_DATE,
MAX(DATE(schmtg.meeting_start_date_time)) AS OPPORTUNITY_NEXT_COMPLETED_SCHEDULED_START_DATE
FROM sales.OPPORTUNITY opportunity
LEFT JOIN sales.internal_contact owner
        ON owner.employee_id = opportunity.opportunity_owner_id
LEFT JOIN sales.opportunity_meeting_link link
        ON link.opportunity_key = opportunity.opportunity_key
LEFT JOIN sales.meeting schmtg
        ON schmtg.meeting_key = link.meeting_key 
        AND schmtg.status_code = 'SCHEDULED'
LEFT JOIN sales.meeting compmtg
        ON compmtg.meeting_key = link.meeting_key 
        AND compmtg.status_code = 'COMPLETED' 
WHERE opportunity.CREATION_DATE_TIME >= CURRENT DATE - 2 YEARS
--WHERE opportunity.OPPORTUNITY_ID = 'OPP0000000485912'
GROUP BY 
opportunity.OPPORTUNITY_ID, 
--OC_METADATA_ID_1, 
--OC_METADATA_ID_2, 
opportunity.ACTIVITY_CONTEXT, 
--opportunity.COMPANY_ORGANIZATION_ID, 
opportunity.CREATION_DATE_TIME, 
opportunity.ESTIMATED_COMPLETION_DATE, 
opportunity.CLOSE_DATE_TIME,
opportunity.CLOSE_YEAR_MONTH, 
opportunity.LAST_MODIFICATION_DATE_TIME, 
opportunity.ESTIMATED_TOTAL_MONTHLY_REVENUE_FROM_SERVICE_ITEMS_AMOUNT, 
opportunity.ESTIMATED_TOTAL_MONTHLY_REVENUE_FROM_SERVICE_ITEMS_CURRENCY_CODE, 
opportunity.GCI_NUMBER, 
--INDUSTRY_CODE, 
opportunity.INVESTMENT_AND_DESIRABILITY_CODE, 
opportunity.IS_PROJECT_BASED, 
opportunity.IS_REQUEST_FOR_INFORMATION, 
opportunity.IS_REQUEST_FOR_PRICE, 
opportunity.IS_REQUEST_FOR_QUOTE, 
--LATEST_EXTERNAL_MEETING_DATE, 
opportunity.LEGAL_NAME, 
opportunity.OPPORTUNITY_DESCRIPTION, 
opportunity.OPPORTUNITY_OWNER_ID,
owner.branch, 
--opportunity.OPPORTUNITY_SOLUTION, 
opportunity.OPPORTUNITY_STATUS_CHANGE_DATE_TIME, 
opportunity.OPPORTUNITY_STATUS_CODE, 
opportunity.OPPORTUNITY_STEP_PROGRESSION_CODE, 
opportunity.OPPORTUNITY_STEP_PROGRESSION_CODE_CHANGE_DATE_TIME, 
opportunity.OPPORTUNITY_TYPE_CODE, 
opportunity.OPPORTUNITY_TYPE_CODE_CHANGE_DATE_TIME, 
opportunity.OWNING_BRANCH_CODE, 
opportunity.PERCENT_OF_ESTIMATED_MONTHLY_REVENUE_FROM_INCUMBENCY_CODE, 
opportunity.PRIMARY_FOCUS_CODE, 
--PROJECT_BASED_DURATION_IN_MONTHS, 
--REASON_LOST_CODE, 
opportunity.SEGMENTATION_CODE, 
opportunity.TRANSITION_STATUS_CODE, 
opportunity.USER_ESTIMATED_TOTAL_MONTHLY_REVENUE_AMOUNT, 
opportunity.USER_ESTIMATED_TOTAL_MONTHLY_REVENUE_AMOUNT_CURRENCY_CODE, 
--INSERT_DTTM, 
--UPDATE_DTTM, 
--CHECKSUM, 
opportunity.TRANSITION_STATUS_SET_TIME, 
opportunity.ACCOUNT_ID, 
opportunity.ACCOUNT_NAME, 
opportunity.CUSTOMER_ACCOUNT_MANAGEMENT_LEVEL_CODE, 
--opportunity.BRANCH_KEY, 
--opportunity.CLIENT_KEY, 
--opportunity.CREATION_DATE_TIME_DATE_KEY, 
--opportunity.ESTIMATED_COMPLETION_DATE_DATE_KEY, 
--opportunity.LAST_MODIFICATION_DATE_TIME_DATE_KEY, 
opportunity.ACTUAL_BID_KICK_OFF_DATE, 
opportunity.BID_RECEIVED_ACT_DATE, 
opportunity.BID_RECEIVED_EST_DATE, 
opportunity.BID_ROUND_ACTUAL_COMPLETION_DATE, 
opportunity.BID_ROUND_ESTIMATED_COMPLETION_DATE, 
opportunity.BID_ROUND_NUMBER, 
opportunity.CUSTOMER_DEBRIEF_ACTUAL_DATE, 
opportunity.CUSTOMER_DEBRIEF_ESTIMATED_DATE, 
opportunity.ESTIMATED_BID_KICK_OFF_DATE, 
opportunity.INVOLVED_BRANCHES, 
opportunity.OUTCOME_RECEIVED_ACTUAL_DATE, 
opportunity.OUTCOME_RECEIVED_ESTIMATED_DATE, 
opportunity.PRODUCTS_AIR_FLAG_YN, 
opportunity.PRODUCTS_CUSTOMS_FLAG_YN, 
opportunity.PRODUCTS_DISTRIBUTION_FLAG_YN, 
opportunity.PRODUCTS_OCEAN_FLAG_YN, 
opportunity.PRODUCTS_ORDER_MANAGEMENT_FLAG_YN, 
opportunity.PRODUCTS_OTHER_FLAG_YN, 
opportunity.PRODUCTS_PROJECT_CARGO_FLAG_YN, 
opportunity.PRODUCTS_TRANSCON_FLAG_YN, 
opportunity.PRODUCT_CODES_FROM_BID, 
opportunity.INDUSTRY_DESCRIPTION, 
opportunity.REASON_LOST_DESCRIPTION, 
opportunity.VERTICAL_INDUSTRY_TEAM_CODE, 
opportunity.ACCOUNT_STATUS, 
opportunity.CUSTOMER_ACCOUNT_OWNER_EMPLOYEE_ID, 
opportunity.CUSTOMER_ACCOUNT_OWNER_NAME, 
opportunity.EXTERNAL_CONTACT_NAMES, 
opportunity.DUNS_NUMBER, 
opportunity.PRODUCTS_INSURANCE_FLAG_YN, 
opportunity.PRODUCT_CODES_FROM_USER, 
opportunity.PRODUCTS_BID_AIR_FLAG_YN, 
opportunity.PRODUCTS_BID_CUSTOMS_FLAG_YN, 
opportunity.PRODUCTS_BID_DISTRIBUTION_FLAG_YN, 
opportunity.PRODUCTS_BID_INSURANCE_FLAG_YN, 
opportunity.PRODUCTS_BID_OCEAN_FLAG_YN, 
opportunity.PRODUCTS_BID_ORDER_MANAGEMENT_FLAG_YN, 
opportunity.PRODUCTS_BID_OTHER_FLAG_YN, 
opportunity.PRODUCTS_BID_PROJECT_CARGO_FLAG_YN, 
opportunity.PRODUCTS_BID_TRANSCON_FLAG_YN, 
opportunity.STEP_PROGRESSION_CODE_20_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_50_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_70_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_90_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_100_DATE_TIME, 
opportunity.STEP_PROGRESSION_CODE_20_DAYS_ON_HOLD
)

SELECT 
opp.OPPORTUNITY_ID, 
--OC_METADATA_ID_1, 
--OC_METADATA_ID_2, 
opp.ACTIVITY_CONTEXT, 
--opp.COMPANY_ORGANIZATION_ID, 
opp.CREATION_DATE_TIME, 
opp.ESTIMATED_COMPLETION_DATE, 
opp.CLOSE_DATE_TIME,
opp.CLOSE_YEAR_MONTH, 
opp.LAST_MODIFICATION_DATE_TIME AS OPPORTUNITY_LAST_MODIFICATION_DATE_TIME, 
opp.ESTIMATED_TOTAL_MONTHLY_REVENUE_FROM_SERVICE_ITEMS_AMOUNT, 
opp.ESTIMATED_TOTAL_MONTHLY_REVENUE_FROM_SERVICE_ITEMS_CURRENCY_CODE, 
opp.GCI_NUMBER, 
--INDUSTRY_CODE, 
opp.INVESTMENT_AND_DESIRABILITY_CODE, 
opp.IS_PROJECT_BASED, 
opp.IS_REQUEST_FOR_INFORMATION, 
opp.IS_REQUEST_FOR_PRICE, 
opp.IS_REQUEST_FOR_QUOTE, 
--LATEST_EXTERNAL_MEETING_DATE, 
opp.LEGAL_NAME, 
opp.OPPORTUNITY_DESCRIPTION, 
opp.OPPORTUNITY_OWNER_ID, 
opp.OPPORTUNITY_OWNER_BRANCH,
--opp.OPPORTUNITY_SOLUTION, 
opp.OPPORTUNITY_STATUS_CHANGE_DATE_TIME, 
opp.OPPORTUNITY_STATUS_CODE, 
opp.OPPORTUNITY_STEP_PROGRESSION_CODE, 
opp.OPPORTUNITY_STEP_PROGRESSION_CODE_CHANGE_DATE_TIME, 
opp.OPPORTUNITY_TYPE_CODE, 
opp.OPPORTUNITY_TYPE_CODE_CHANGE_DATE_TIME, 
opp.OWNING_BRANCH_CODE, 
opp.PERCENT_OF_ESTIMATED_MONTHLY_REVENUE_FROM_INCUMBENCY_CODE, 
opp.PRIMARY_FOCUS_CODE, 
--PROJECT_BASED_DURATION_IN_MONTHS, 
--REASON_LOST_CODE, 
opp.SEGMENTATION_CODE, 
opp.TRANSITION_STATUS_CODE, 
opp.USER_ESTIMATED_TOTAL_MONTHLY_REVENUE_AMOUNT, 
opp.USER_ESTIMATED_TOTAL_MONTHLY_REVENUE_AMOUNT_CURRENCY_CODE, 
--INSERT_DTTM, 
--UPDATE_DTTM, 
--CHECKSUM, 
opp.TRANSITION_STATUS_SET_TIME, 
opp.ACCOUNT_ID, 
opp.ACCOUNT_NAME, 
opp.CUSTOMER_ACCOUNT_MANAGEMENT_LEVEL_CODE, 
--opp.BRANCH_KEY, 
--opp.CLIENT_KEY, 
--opp.CREATION_DATE_TIME_DATE_KEY, 
--opp.ESTIMATED_COMPLETION_DATE_DATE_KEY, 
--opp.LAST_MODIFICATION_DATE_TIME_DATE_KEY, 
opp.ACTUAL_BID_KICK_OFF_DATE, 
opp.BID_RECEIVED_ACT_DATE, 
opp.BID_RECEIVED_EST_DATE, 
opp.BID_ROUND_ACTUAL_COMPLETION_DATE, 
opp.BID_ROUND_ESTIMATED_COMPLETION_DATE, 
opp.BID_ROUND_NUMBER, 
opp.CUSTOMER_DEBRIEF_ACTUAL_DATE, 
opp.CUSTOMER_DEBRIEF_ESTIMATED_DATE, 
opp.ESTIMATED_BID_KICK_OFF_DATE, 
opp.INVOLVED_BRANCHES, 
opp.OUTCOME_RECEIVED_ACTUAL_DATE, 
opp.OUTCOME_RECEIVED_ESTIMATED_DATE, 
opp.PRODUCTS_AIR_FLAG_YN, 
opp.PRODUCTS_CUSTOMS_FLAG_YN, 
opp.PRODUCTS_DISTRIBUTION_FLAG_YN, 
opp.PRODUCTS_OCEAN_FLAG_YN, 
opp.PRODUCTS_ORDER_MANAGEMENT_FLAG_YN, 
opp.PRODUCTS_OTHER_FLAG_YN, 
opp.PRODUCTS_PROJECT_CARGO_FLAG_YN, 
opp.PRODUCTS_TRANSCON_FLAG_YN, 
opp.PRODUCT_CODES_FROM_BID, 
opp.INDUSTRY_DESCRIPTION, 
opp.REASON_LOST_DESCRIPTION, 
opp.VERTICAL_INDUSTRY_TEAM_CODE, 
opp.ACCOUNT_STATUS, 
opp.CUSTOMER_ACCOUNT_OWNER_EMPLOYEE_ID, 
opp.CUSTOMER_ACCOUNT_OWNER_NAME, 
opp.EXTERNAL_CONTACT_NAMES, 
opp.DUNS_NUMBER, 
opp.PRODUCTS_INSURANCE_FLAG_YN, 
opp.PRODUCT_CODES_FROM_USER, 
opp.PRODUCTS_BID_AIR_FLAG_YN, 
opp.PRODUCTS_BID_CUSTOMS_FLAG_YN, 
opp.PRODUCTS_BID_DISTRIBUTION_FLAG_YN, 
opp.PRODUCTS_BID_INSURANCE_FLAG_YN, 
opp.PRODUCTS_BID_OCEAN_FLAG_YN, 
opp.PRODUCTS_BID_ORDER_MANAGEMENT_FLAG_YN, 
opp.PRODUCTS_BID_OTHER_FLAG_YN, 
opp.PRODUCTS_BID_PROJECT_CARGO_FLAG_YN, 
opp.PRODUCTS_BID_TRANSCON_FLAG_YN, 
opp.STEP_PROGRESSION_CODE_20_DATE_TIME, 
opp.STEP_PROGRESSION_CODE_50_DATE_TIME, 
opp.STEP_PROGRESSION_CODE_70_DATE_TIME, 
opp.STEP_PROGRESSION_CODE_90_DATE_TIME, 
opp.STEP_PROGRESSION_CODE_100_DATE_TIME, 
opp.STEP_PROGRESSION_CODE_20_DAYS_ON_HOLD,
opp.OPPORTUNITY_MEETING_COUNT,
opp.OPPORTUNITY_FIRST_COMPLETED_MTG_START_DATE,
opp.OPPORTUNITY_LAST_COMPLETED_MTG_START_DATE,
opp.OPPORTUNITY_NEXT_COMPLETED_SCHEDULED_START_DATE,
--OPPORTUNITY_SERVICE_ITEM_KEY, 
osi.OPPORTUNITY_SERVICE_ITEM_ID, 
--osi.OPPORTUNITY_ID, 
--OSIC_METADATA_ID_1, 
--OSIC_METADATA_ID_2, 
osi.ACTUAL_CLOSE_DATE, 
osi.DELETION_DATE_TIME, 
osi.DENSITY_CODE, 
osi.DESTINATION_PORT_CODE, 
dest.branch_code AS DESTINATION_BRANCH,
osi.ESTIMATED_CLOSE_DATE, 
osi.IS_DANGEROUS_GOODS, 
osi.IS_DELIVERY, 
osi.IS_EXPORT_DECLARATION, 
osi.IS_IMPORT_DECLARATION, 
osi.IS_PICKUP, 
osi.IS_TRANSACTIONAL_INSURANCE, 
osi.LAST_MODIFICATION_DATE_TIME AS OPPORTUNITY_SERVICE_ITEM_LAST_MODIFICATION_DATE_TIME, 
osi.OPPORTUNITY_SERVICE_ITEM_PRODUCT_CODE, 
osi.OPPORTUNITY_SERVICE_ITEM_STATUS_CODE, 
osi.OPPORTUNITY_SERVICE_ITEM_STEP_PROGRESSION_CODE, 
osi.ORIGIN_PORT_CODE,
origin.branch_code AS ORIGIN_BRANCH,
osi.SELL_SERVICE_CODE, 
osi.SERVICE_LEVEL_CODE, 
osi.SERVICE_LOCATION_CODE, 
service.branch_code AS SERVICE_LOCATION_BRANCH,
osi.SERVICE_OFFICE_CODE, 
om.branch_code AS SERVICE_OFFICE_BRANCH,
osi.SYSTEM_CALCULATED_DESTINATION_MONTHLY_REVENUE_AMOUNT, 
osi.SYSTEM_CALCULATED_DESTINATION_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.SYSTEM_CALCULATED_ORIGIN_MONTHLY_REVENUE_AMOUNT, 
osi.SYSTEM_CALCULATED_ORIGIN_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.SYSTEM_CALCULATED_SERVICE_LOCATION_MONTHLY_REVENUE_AMOUNT, 
osi.SYSTEM_CALCULATED_SERVICE_LOCATION_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.SYSTEM_CALCULATED_SERVICE_OFFICE_MONTHLY_REVENUE_AMOUNT, 
osi.SYSTEM_CALCULATED_SERVICE_OFFICE_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.SYSTEM_CALCULATED_TOTAL_ESTIMATED_MONTHLY_REVENUE_AMOUNT, 
osi.SYSTEM_CALCULATED_TOTAL_ESTIMATED_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.TRANSACTIONAL_INSURANCE_TOTAL_INSURED_VALUE_AMOUNT, 
osi.TRANSACTIONAL_INSURANCE_TOTAL_INSURED_VALUE_CURRENCY_CODE, 
osi.USER_ESTIMATED_DESTINATION_MONTHLY_REVENUE_AMOUNT, 
osi.USER_ESTIMATED_DESTINATION_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.USER_ESTIMATED_ORIGIN_MONTHLY_REVENUE_AMOUNT, 
osi.USER_ESTIMATED_ORIGIN_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.USER_ESTIMATED_SERVICE_LOCATION_MONTHLY_REVENUE_AMOUNT, 
osi.USER_ESTIMATED_SERVICE_LOCATION_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.USER_ESTIMATED_SERVICE_OFFICE_MONTHLY_REVENUE_AMOUNT, 
osi.USER_ESTIMATED_SERVICE_OFFICE_MONTHLY_REVENUE_CURRENCY_CODE, 
osi.USER_TOTAL_ESTIMATED_MONTHLY_REVENUE_FROM_USER_ENTERED_REVENUE_AMOUNT, 
osi.USER_TOTAL_ESTIMATED_MONTHLY_REVENUE_FROM_USER_ENTERED_REVENUE_CURRENCY_CODE, 
osi.WIN_PROBABILITY_CODE, 
osi.MONTHLY_QUANTITY, 
osi.MONTHLY_QUANTITY_UNIT_CODE, 
osi.MONTHLY_TRANSACTIONS, 
osi.PERCENT_AWARDED_VOLUME_CODE, 
osi.PERCENT_INCUMBENCY_CODE, 
osi.PERCENT_TARGETED_CODE
FROM opp
LEFT JOIN sales.OPPORTUNITY_SERVICE_ITEM osi
        ON osi.opportunity_id = opp.opportunity_id
LEFT JOIN dimensions.port_branch_map origin
        ON origin.port_code = osi.origin_port_code
LEFT JOIN dimensions.new_branch_dimension origin_nbd
        ON origin.branch_key = origin_nbd.branch_key
        AND origin_nbd.operational_branch_indicator = 'Y'
LEFT JOIN dimensions.port_branch_map dest
        ON dest.port_code = osi.destination_port_code
LEFT JOIN dimensions.new_branch_dimension dest_nbd
        ON dest.branch_key = dest_nbd.branch_key
        AND dest_nbd.operational_branch_indicator = 'Y'
LEFT JOIN dimensions.port_branch_map service
        ON service.port_code = osi.service_location_code
LEFT JOIN dimensions.new_branch_dimension service_nbd
        ON service.branch_key = service_nbd.branch_key
        AND service_nbd.operational_branch_indicator = 'Y'
LEFT JOIN dimensions.port_branch_map om
        ON om.port_code = osi.service_office_code
LEFT JOIN dimensions.new_branch_dimension om_nbd
        ON om.branch_key = om_nbd.branch_key
        AND om_nbd.operational_branch_indicator = 'Y'                    
    """ 
     , con)
    
    df.columns = map(lambda x: str(x).upper(), df.columns)
    myfile = r'F:\FTP\DWSales\stage_opportunityserviceitems.csv'
    
    if os.path.isfile(myfile):
        os.remove(myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_opportunityserviceitems.csv', index=None, header=True) #outputs csv file to a file directory
    else:    ## Show an error ##
        print("Error: %s file not found" % myfile)
        df.to_csv(r'F:\FTP\DWSales\stage_opportunityserviceitems.csv', index=None, header=True) #outputs csv file to a file directory                                                       
    





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

