/*================================================================================================================================== 
Created By: Jimmy Nguyen 
Created Date: 2020-02-20
Data Source: VIZCRM.sales.opportunity_meeting_link 
Data Destination: DWSales.stage.STAGE_OPPORTUNITY_MEETING_BRIDGE 
Description: This query loads the stage.STAGE_OPPORTUNITY_MEETING_BRIDGE  table in the DWSales database
                
Change Log: Who, What, When, What     
-- 
-
==================================================================================================================================*/

SELECT 
        OPPORTUNITY_ID
        ,MEETING_ID
FROM sales.opportunity_meeting_link   