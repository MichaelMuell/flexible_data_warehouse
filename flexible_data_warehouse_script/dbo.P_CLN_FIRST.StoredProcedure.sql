/****** Object:  StoredProcedure [dbo].[P_CLN_FIRST]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_FIRST] AS 

DECLARE @Table TABLE
(
TableName VARCHAR(50),
Id int identity(1,1)
)

INSERT INTO @Table
Select DISTINCT table_name From INFORMATION_SCHEMA.COLUMNS 
Where LEFT(TABLE_NAME,2) = 'IN' AND TABLE_NAME <> 'ING_FI5000'
AND TABLE_SCHEMA = 'dbo'

DECLARE @max int
DECLARE @SQL VARCHAR(MAX) 
DECLARE @TableName VARCHAR(50)
DECLARE @id int = 1

select @max = MAX(Id) from @Table

WHILE (@id <= @max)
BEGIN

SELECT @TableName = TableName FROM @Table WHERE Id = @id
SET @SQL =     'DROP TABLE IF EXISTS CLN'+Substring(@Tablename,4,20)+';
				SELECT * INTO dbo.CLN'+Substring(@Tablename,4,20)+' FROM '+@TableName+''

--PRINT(@SQL) 
EXEC(@SQL)
SET @id = @id +1
END


IF COL_LENGTH ('dbo.CLN_T001','COMPANY_NAME_SHORT') IS NULL
BEGIN
ALTER TABLE CLN_T001
ADD COMPANY_NAME_SHORT varchar(MAX)
END

IF COL_LENGTH ('dbo.CLN_BSAK','CLEARING_DOCUMENT_YEAR') IS NULL
BEGIN
ALTER TABLE CLN_BSAK
ADD CLEARING_DOCUMENT_YEAR varchar(MAX)
END

IF COL_LENGTH ('dbo.CLN_BSIK','CLEARING_DOCUMENT_YEAR') IS NULL
BEGIN
ALTER TABLE CLN_BSIK
ADD CLEARING_DOCUMENT_YEAR varchar(MAX)
END

IF COL_LENGTH ('dbo.CLN_REGUP','PAYMENT_DOCUMENT_YEAR') IS NULL
BEGIN
ALTER TABLE CLN_REGUP
ADD PAYMENT_DOCUMENT_YEAR varchar(MAX)
END
GO
