USE [DWSales]
GO

/****** Object:  StoredProcedure [dbo].[spETL_MEETING_CONTACTS]    Script Date: 2/20/2020 5:14:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jimmy Nguyen
-- Create date: 2020-02-18
-- Data Source: DWSales.stage.STAGE_MEETING
-- Data Destination: DWSales.sales.MEETING_CONTACTS
-- Description: This query loads the DWSales.sales.MEETING_CONTACTS in the DWSales database              
-- Change Log: Who, What, When, What   

-- =============================================
CREATE PROCEDURE [dbo].[spETL_MEETING_CONTACTS]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	IF((SELECT COUNT(*) FROM DWSales.stage.STAGE_MEETING) > 2)
        BEGIN
                Alter Table DWSales.sales.OPPORTUNITY_SERVICE_ITEM Drop Constraint IF EXISTS FK_OPPORTUNITY_SERVICE_ITEM_OPPORTUNITY
                Alter Table DWSales.sales.OPPORTUNITY_MEETING_BRIDGE Drop Constraint IF EXISTS FK_OPPORTUNITY_MEETING_BRIDGE_OPPORTUNITY
				TRUNCATE TABLE DWSales.sales.MEETING_CONTACTS
                
                INSERT INTO DWSales.sales.MEETING_CONTACTS (MEETING_KEY, MEETING_CONTACT_ROLES, EMPLOYEE_ID)
                SELECT DISTINCT
                        mc.MEETING_KEY,
                        --m.MEETING_ID, 
                        m.MEETING_CONTACT_ROLES,
                        m.MEETING_CONTACT_ROLE_EMPLOYEE_ID AS EMPLOYEE_ID
                        FROM DwSales.stage.STAGE_MEETING m
                        INNER JOIN (SELECT MEETING_KEY, MEETING_ID FROM DwSales.sales.MEETING) mc
                                ON mc.meeting_id = m.meeting_id
        END

	
END
GO


