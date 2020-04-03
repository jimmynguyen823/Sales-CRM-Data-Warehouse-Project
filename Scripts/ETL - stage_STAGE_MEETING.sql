/*================================================================================================================================== 
Created By: Jimmy Nguyen 
Created Date: 2020-02-18
Datasource: VIZCRM 
Data Destination: DWSales  
Description: This query loads the stage.STAGE_MEETING table in the DWSales database
                
Change Log: Who, What, When, What     
-- 
-
==================================================================================================================================*/


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