USE [DWSales]
GO

/****** Object:  StoredProcedure [dbo].[spETL_SALESPERSON_CURRENT_DIMENSION]    Script Date: 2/20/2020 5:10:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jimmy Nguyen
-- Create date: 2020-02-18
-- Data Source: Sales.sales.SALESPERSON_DIMENSION
-- Data Destination: DWSales.dbo.SALESPERSON_CURRENT_DIMENSION
-- Description: This query loads the DWSales.dbo.SALESPERSON_CURRENT_DIMENSION table in the DWSales database              
-- Change Log: Who, What, When, What   

-- =============================================
CREATE PROCEDURE [dbo].[spETL_SALESPERSON_CURRENT_DIMENSION]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	IF((SELECT COUNT(*) FROM Sales.sales.SALESPERSON_DIMENSION) > 2)
        BEGIN
			TRUNCATE TABLE DWSales.dbo.SALESPERSON_CURRENT_DIMENSION
            INSERT INTO DWSales.dbo.SALESPERSON_CURRENT_DIMENSION (LOAD_YEAR_MONTH, LOAD_YEAR_MONTH_NAME, SALESPERSON_EMPLOYEE_ID, SALESPERSON_NAME, SALESPERSON_TITLE, SALESPERSON_BRANCH, SALESPERSON_DISTRICT, SALESPERSON_COUNTRY_CODE, SALESPERSON_COUNTRY, SALESPERSON_REGION_CODE, SALESPERSON_REGION, SALESPERSON_GEO, SALESPERSON_START_DATE, SALESPERSON_TENURE_YEARS, SALESPERSON_TENURE_YEARS_ROUND, SALESPERSON_STATUS, SALESPERSON_TRANSFERRED_FROM, REPORTING_YN, BRIDGE_FLAG_YN)
                    SELECT 
                             LOAD_YEAR_MONTH = CAST(sp.load_year_and_month AS INT)
                            ,LOAD_YEAR_MONTH_NAME =  CAST(sp.load_year_and_month_name AS NVARCHAR(50))
                            ,SALESPERSON_EMPLOYEE_ID = CAST(sp.salesperson_id AS BIGINT)
                            ,SALESPERSON_NAME = CAST(sp.salesperson_name AS NVARCHAR(50))
                            ,SALESPERSON_TITLE = CAST(sp.salesperson_title_code AS NVARCHAR(50))
                            ,SALESPERSON_BRANCH = CAST(sp.salesperson_branch AS NVARCHAR(50))
                            ,SALESPERSON_DISTRICT = CAST(sp.salesperson_district AS NVARCHAR(50))
                            ,SALESPERSON_COUNTRY_CODE = CAST(sp.salesperson_country_code AS NVARCHAR(50))
                            ,SALESPERSON_COUNTRY = CAST(sp.salesperson_country AS NVARCHAR(50))
                            ,SALESPERSON_REGION_CODE = CAST(sp.salesperson_region_code AS NVARCHAR(50))
                            ,SALESPERSON_REGION  = CAST(sp.salesperson_region AS NVARCHAR(50))
                            ,SALESPERSON_GEO = CAST(sp.salesperson_geo AS NVARCHAR(50))
                            ,SALESPERSON_START_DATE = CAST(sp.salesperson_start_date AS DATE)
                            ,SALESPERSON_TENURE_YEARS = CAST(sp.salesperson_years AS DECIMAL(18,2))
                            ,SALESPERSON_TENURE_YEARS_ROUND = CAST(sp.salesperson_tenure_years AS INT)
                            ,SALESPERSON_STATUS = CAST(sp.salesperson_status AS NVARCHAR(50))
                            ,SALESPERSON_TRANSFERRED_FROM = CAST(sp.salesperson_transferred_from AS NVARCHAR(50))
                            ,REPORTING_YN = CAST(sp.reporting_yn AS NVARCHAR(10))
                            ,BRIDGE_FLAG_YN = CAST(sp.bridge_flag_yn AS NVARCHAR(10))
                    FROM Sales.sales.SALESPERSON_DIMENSION sp

		--EXEC spLogUpdate 'Action Items Stage To Final',@@ROWCOUNT
        END
              
END
GO


