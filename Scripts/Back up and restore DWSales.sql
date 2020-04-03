/************************************************ 
1) Make a copy of the empty database 
before starting the ETL process
************************************************/

BACKUP DATABASE [DWSales] 
TO  DISK = 
N'F:\FTP\DWSales\DWSales_20200221.bak'
GO

/************************************************ 
2) Send the file to other team members
and tell them they can restore the database
with this code...
************************************************/

/*
-- Check to see if they already have a copy...
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'DWSales')
  BEGIN
  -- If they do, they need to close connections to the DWPubsSales database, with this code!
    ALTER DATABASE [DWPubsSales] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
  END
*/
-- Now now restore the Empty database...
USE Master 
RESTORE DATABASE [DWSales] 
FROM DISK = 
N'F:\FTP\DWSales\DWSales_20200221.bak'
WITH REPLACE
GO
