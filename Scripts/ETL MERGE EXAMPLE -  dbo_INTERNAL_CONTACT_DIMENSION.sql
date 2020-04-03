/*
INSERT INTO DwSales.stage.STAGE_INTERNAL_CONTACT (EMPLOYEE_ID, USER_ID, BRANCH, DEPARTMENT, FIRST_NAME, LAST_NAME, DISPLAY_NAME, STATUS, EMAIL, TITLE, SUPERVISOR_EMPLOYEE_ID, EMPLOYMENT_TYPE, MANAGERIAL_LEVEL, LOCATION_DETAIL)
VALUES
	(3751951,'bjs-jennyj','BJS','Sales','Jenny','Jian','Jenny Jian','Suspended', 'jenny.jian@expeditors.com','District Sales Executive-Sales&Marketing,BJS',3729696,'Employee','Lead',NULL)
	,(3751955,'mil-sabinar','MIL','Accounting','Sabina','Rocca','Sabina Rocca','Active','sabina.rocca@expeditors.com','Accounting Agent',3700801,'Employee',NULL,NULL)


TRUNCATE TABLE DwSales.dbo.INTERNAL_CONTACT_DIMENSION

SELECT * FROM DwSales.stage.STAGE_INTERNAL_CONTACT
SELECT * FROM DwSales.dbo.INTERNAL_CONTACT_DIMENSION 
where IS_CURRENT = 'N' ORDER BY employee_id, start_date



UPDATE DwSales.stage.STAGE_INTERNAL_CONTACT
SET [status] = 'Active',
	title =  'Astronaut' ,
	branch = 'SEA',
	email = 'jenny.han@expeditors.com',
	display_name = 'Jenny Han',
	user_id = 'sea-jennyh'
	where internal_contact_key = 1
*/
--https://www.databasejournal.com/features/mssql/managing-slowly-changing-dimension-with-merge-statement-in-sql-server.html


MERGE INTO DwSales.dbo.INTERNAL_CONTACT_DIMENSION AS t -- t is for target table
USING DWSales.stage.STAGE_INTERNAL_CONTACT AS s
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
Go


INSERT INTO DwSales.dbo.INTERNAL_CONTACT_DIMENSION (Employee_id, user_id, branch, department, first_name, last_name, display_name, [status], email, title, supervisor_employee_id, employment_type, managerial_level,location_detail, [start_date],end_date, is_current)
SELECT Employee_id, user_id, branch, department, first_name, last_name, display_name, [status], email, title, supervisor_employee_id, employment_type, managerial_level,location_detail, [start_date],end_date, is_current
FROM
(MERGE INTO DwSales.dbo.INTERNAL_CONTACT_DIMENSION AS t -- t is for target table
USING DWSales.stage.STAGE_INTERNAL_CONTACT AS s
  -- s is for source table
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
GO
