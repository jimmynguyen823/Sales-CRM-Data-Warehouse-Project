USE [DWSales]
GO

/****** Object:  StoredProcedure [dbo].[spETL_INTERNAL_CONTACT_DIMENSION]    Script Date: 2/20/2020 5:09:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jimmy Nguyen
-- Create date: 2020-02-19
-- Data Source: DWSales.stage.STAGE_OPPORTUNITY_SERVICE_ITEM
-- Data Destination: DWSales.dbo.INTERNAL_CONTACT_DIMENSION
-- Description: This query loads the dbo.INTERNAL_CONTACT_DIMENSION table in the DWSales database              
-- Change Log: Who, What, When, What   

-- =============================================
CREATE PROCEDURE [dbo].[spETL_INTERNAL_CONTACT_DIMENSION]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	IF((SELECT COUNT(*) FROM Sales.sales.IDENTITY_DIMENSION) > 10)
        BEGIN
	-- Implements Slowly Changing Dimension Type 1 
		MERGE INTO DwSales.dbo.INTERNAL_CONTACT_DIMENSION AS t -- t is for target table
		USING
		(
		SELECT 
				CAST(IDENTITY_NUMBER AS BIGINT) AS EMPLOYEE_ID,
				CAST([USER_ID] AS NVARCHAR(50)) [USER_ID],
				CAST(BRANCH AS NVARCHAR(50)) BRANCH,
				CASE WHEN DEPARTMENT = '' THEN NULL ELSE CAST(DEPARTMENT AS NVARCHAR(50)) END AS DEPARTMENT,
				CAST(FIRST_NAME AS NVARCHAR(100)) FIRST_NAME,
				CAST(LAST_NAME AS NVARCHAR(100)) LAST_NAME,
				CAST(DISPLAY_NAME AS NVARCHAR(100)) DISPLAY_NAME,
				CAST(STATUS AS NVARCHAR(50)) AS STATUS,
				CASE WHEN MAIL = '' THEN NULL ELSE CAST(MAIL AS NVARCHAR(150)) END AS EMAIL,
				CASE WHEN TITLE = '' THEN NULL ELSE CAST(TITLE AS NVARCHAR(100)) END AS TITLE,
				CAST(SUPERVISOR_IDM AS BIGINT) AS SUPERVISOR_EMPLOYEE_ID,
				CAST(EMPLOYMENT_TYPE AS NVARCHAR(50)) EMPLOYMENT_TYPE,
				CASE WHEN [LEVEL] = '' THEN NULL ELSE CAST([LEVEL] AS NVARCHAR(150)) END AS MANAGERIAL_LEVEL,
				CASE WHEN LOCATION = '' THEN NULL ELSE CAST(LOCATION AS NVARCHAR(100)) END AS LOCATION_DETAIL
		FROM Sales.sales.IDENTITY_DIMENSION
		) AS s 
		  -- s is for source table
			ON t.employee_id = S.employee_id     
		  
		WHEN MATCHED  AND t.is_current = 'Y' -- When the Employee ID match between source and target table; SCD 1 - update the values below if the fields below don't match 
			AND (s.department <> t.department
			OR s.first_name <> t.first_name 
			OR s.last_name <> t.last_name 
			OR s.status <> t.status 
			OR s.title <> t.title
			OR s.supervisor_employee_id <> t.supervisor_employee_id
			OR s.employment_type <> t.employment_type 
			OR s.managerial_level <> t.managerial_level
			OR s.location_detail <> t.location_detail)
		THEN 
			UPDATE
			SET t.department = s.department
				,t.first_name = s.first_name
				,t.last_name = s.last_name
				,t.status = s.status
				,t.title = s.title
				,t.supervisor_employee_id = s.supervisor_employee_id 
				,t.employment_type = s.employment_type
				,t.managerial_level = s.managerial_level
				,t.location_detail = s.location_detail
				,t.insert_date = GETDATE() 
		WHEN NOT MATCHED BY SOURCE -- The Employee ID in the Target table is not in the Source table then delete the row
			THEN 
				DELETE
		;
			-- Implements Slowly Changing Dimension Type 2
		INSERT INTO DwSales.dbo.INTERNAL_CONTACT_DIMENSION (Employee_id, user_id, branch, department, first_name, last_name, display_name, [status], email, title, supervisor_employee_id, employment_type, managerial_level,location_detail, [start_date],end_date, is_current)
		SELECT Employee_id, user_id, branch, department, first_name, last_name, display_name, [status], email, title, supervisor_employee_id, employment_type, managerial_level,location_detail, [start_date],end_date, is_current
		FROM
		(MERGE INTO DwSales.dbo.INTERNAL_CONTACT_DIMENSION AS t -- t is for target table
		USING
		(
		SELECT 
				CAST(IDENTITY_NUMBER AS BIGINT) AS EMPLOYEE_ID,
				CAST([USER_ID] AS NVARCHAR(50)) [USER_ID],
				CAST(BRANCH AS NVARCHAR(50)) BRANCH,
				CASE WHEN DEPARTMENT = '' THEN NULL ELSE CAST(DEPARTMENT AS NVARCHAR(50)) END AS DEPARTMENT,
				CAST(FIRST_NAME AS NVARCHAR(100)) FIRST_NAME,
				CAST(LAST_NAME AS NVARCHAR(100)) LAST_NAME,
				CAST(DISPLAY_NAME AS NVARCHAR(100)) DISPLAY_NAME,
				CAST(STATUS AS NVARCHAR(50)) AS STATUS,
				CASE WHEN MAIL = '' THEN NULL ELSE CAST(MAIL AS NVARCHAR(150)) END AS EMAIL,
				CASE WHEN TITLE = '' THEN NULL ELSE CAST(TITLE AS NVARCHAR(100)) END AS TITLE,
				CAST(SUPERVISOR_IDM AS BIGINT) AS SUPERVISOR_EMPLOYEE_ID,
				CAST(EMPLOYMENT_TYPE AS NVARCHAR(50)) EMPLOYMENT_TYPE,
				CASE WHEN [LEVEL] = '' THEN NULL ELSE CAST([LEVEL] AS NVARCHAR(150)) END AS MANAGERIAL_LEVEL,
				CASE WHEN LOCATION = '' THEN NULL ELSE CAST(LOCATION AS NVARCHAR(100)) END AS LOCATION_DETAIL
		FROM Sales.sales.IDENTITY_DIMENSION
		) AS s 
			ON t.employee_id = S.employee_id      

		WHEN NOT MATCHED THEN -- The employee ID in the source table is not found in the target table; Add new row
			INSERT (Employee_id, user_id, branch, department, first_name, last_name, display_name, [status], email, title, supervisor_employee_id, employment_type, managerial_level,location_detail, [start_date],end_date, is_current)
			VALUES(s.EMPLOYEE_ID, s.[USER_ID], s.BRANCH, s.DEPARTMENT, s.FIRST_NAME, s.LAST_NAME, s.DISPLAY_NAME, s.[STATUS], s.EMAIL, s.TITLE, s.SUPERVISOR_EMPLOYEE_ID, s.EMPLOYMENT_TYPE, s.MANAGERIAL_LEVEL, s.LOCATION_DETAIL,GETDATE(), null, 'Y')

		WHEN MATCHED AND t.is_current = 'Y' 
			AND (ISNULL(t.[user_id], '') != ISNULL(s.[user_id], '') 
			OR ISNULL(t.branch, '') != ISNULL(s.branch, '')
			OR ISNULL(t.display_name, '') != ISNULL(s.display_name, '')
			OR ISNULL(t.email, '') != ISNULL(s.email, '')) THEN
			UPDATE 
			SET t.is_current= 'N', t.end_date = GETDATE()
			OUTPUT $Action Action_Taken, s.[employee_id], s.[user_id], s.BRANCH, s.DEPARTMENT, s.FIRST_NAME, s.LAST_NAME, s.DISPLAY_NAME, s.[STATUS], s.EMAIL, s.TITLE, s.SUPERVISOR_EMPLOYEE_ID, s.EMPLOYMENT_TYPE, s.MANAGERIAL_LEVEL, s.LOCATION_DETAIL,GETDATE() AS [start_date], null as end_date, 'Y' as is_current
		) AS MERGE_OUT
		WHERE MERGE_OUT.Action_Taken = 'UPDATE';
		
	END
END
GO


