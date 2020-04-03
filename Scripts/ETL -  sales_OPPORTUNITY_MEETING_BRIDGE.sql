SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Jimmy Nguyen
-- Create date: 2020-02-20
-- Data Source: DWSales.stage.STAGE_OPPORTUNITY_MEETING_BRIDGE
-- Data Destination: DWSales.sales.OPPORTUNITY_MEETING_BRIDGE
-- Description: This query loads the sales.OPPORTUNITY_MEETING_BRIDGE in the DWSales database              
-- Change Log: Who, What, When, What   

-- =============================================
CREATE PROCEDURE dbo.spETL_OPPORTUNITY_MEETING_BRIDGE

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	IF((SELECT COUNT(*) FROM DWSales.stage.STAGE_OPPORTUNITY_MEETING_BRIDGE) > 5)
        BEGIN 
                ALTER TABLE DWSales.sales.OPPORTUNITY_MEETING_BRIDGE DROP CONSTRAINT IF EXISTS FK_OPPORTUNITY_MEETING_BRIDGE_MEETING
                Alter Table DWSales.sales.OPPORTUNITY_MEETING_BRIDGE Drop Constraint IF EXISTS FK_OPPORTUNITY_MEETING_BRIDGE_OPPORTUNITY
                TRUNCATE TABLE DwSales.sales.OPPORTUNITY_MEETING_BRIDGE
                
                INSERT INTO DwSales.sales.OPPORTUNITY_MEETING_BRIDGE (OPPORTUNITY_KEY, MEETING_KEY)
                SELECT 
                        --b.OPPORTUNITY_ID
                        o.OPPORTUNITY_KEY
                        ,m.MEETING_KEY
                        --,b.MEETING_ID
                FROM DWSales.stage.STAGE_OPPORTUNITY_MEETING_BRIDGE b
                INNER JOIN DwSales.sales.opportunity o
                        ON b.opportunity_id = o.opportunity_id
                INNER JOIN DwSales.sales.meeting m
                        ON m.meeting_id = b.meeting_id
        END
END
GO
