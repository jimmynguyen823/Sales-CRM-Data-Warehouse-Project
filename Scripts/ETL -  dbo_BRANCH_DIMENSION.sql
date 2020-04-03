USE [DWSales]
GO

/****** Object:  StoredProcedure [dbo].[spETL_BRANCH_DIMENSION]    Script Date: 2/20/2020 5:09:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jimmy Nguyen
-- Create date: 2020-02-18
-- Data Source: Sales.sales.BRANCH_DIMENSION
-- Data Destination: DWSales.dbo.BRANCH_DIMENSION
-- Description: This query loads the dbo.BRANCH_DIMENSION table in the DWSales database              
-- Change Log: Who, What, When, What   

-- =============================================
CREATE PROCEDURE [dbo].[spETL_BRANCH_DIMENSION]
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	IF((SELECT COUNT(*) FROM Sales.sales.BRANCH_DIMENSION) > 2)
        BEGIN
			TRUNCATE TABLE DWSales.dbo.BRANCH_DIMENSION
            INSERT INTO DWSales.dbo.BRANCH_DIMENSION (BRANCH_CODE, BRANCH_NUMBER, BRANCH_NAME, DISTRICT, COUNTRY_CODE, COUNTRY_NAME, REGION_PL_CODE, REGION_PL_NAME, REGION_GEO_NAME1, REGION_GEO_NAME2, REGION_GEO_NAME3, ALLOCATION_REGION, ACCOUNTING_COMPANY, CURRENCY, OPERATIONAL_BRANCH_INDICATOR)
                    SELECT 
                            CAST(BRANCH_CODE AS NVARCHAR(25)) BRANCH_CODE
                            ,CAST(BRANCH_NUMBER AS NVARCHAR(50)) BRANCH_NUMBER
                            ,CAST(BRANCH_NAME AS NVARCHAR(100)) BRANCH_NAME
                            ,CAST(DISTRICT AS NVARCHAR(50)) DISTRICT
                            ,CAST(COUNTRY_CODE AS NVARCHAR(50)) COUNTRY_CODE
                            ,CAST(COUNTRY_NAME AS NVARCHAR(50)) COUNTRY_NAME
                            ,CAST(REGION_PL_CODE AS NVARCHAR(50)) REGION_PL_CODE
                            ,CAST(REGION_PL_NAME AS NVARCHAR(50)) REGION_PL_NAME 
                            ,CAST(REGION_GEO_NAME1 AS NVARCHAR(50)) REGION_GEO_NAME1
                            ,CAST(REGION_GEO_NAME2 AS NVARCHAR(50)) REGION_GEO_NAME2
                            ,CAST(REGION_GEO_NAME3 AS NVARCHAR(50)) REGION_GEO_NAME3
                            ,CAST(ALLOCATION_REGION AS NVARCHAR(50)) ALLOCATION_REGION
                            ,CAST(ACCOUNTING_COMPANY AS NVARCHAR(50)) ACCOUNTING_COMPANY
                            ,CAST(CURRENCY AS NVARCHAR(50)) CURRENCY
                            ,CAST(OPERATIONAL_BRANCH_INDICATOR AS NVARCHAR(50)) OPERATIONAL_BRANCH_INDICATOR
                            FROM Sales.sales.branch_dimension

			--EXEC spLogUpdate 'Action Items Stage To Final',@@ROWCOUNT
        END
       
END
GO


