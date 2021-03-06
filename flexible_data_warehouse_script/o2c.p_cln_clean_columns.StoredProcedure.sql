/****** Object:  StoredProcedure [o2c].[p_cln_clean_columns]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_cln_clean_columns] @schema varchar(max) as

declare @table table
(
tablename varchar(50), 
columnname varchar(50),
id int identity(1,1)
)

insert into @table(tablename,columnname)
select table_name, column_name from config 
where FUNCTION_NAME = 'CLEAN_COLUMNS' 
and DB_SCHEMA = @schema

declare @max int
declare @sql varchar(max) 
declare @tablename varchar(50)
declare @columnname varchar(50) 
declare @id int = 1

select @max = max(id) from @table

while (@id <= @max)
begin

select @tablename = tablename, @columnname =columnname from @table where id = @id

set @sql =  'update '+@schema+'.'+@tablename+' set '+@columnname+' = replace('+@columnname+','','','''');
			 update '+@schema+'.'+@tablename+' set '+@columnname+' = replace('+@columnname+',''"'','''');	'

exec(@sql)

set @id = @id +1
end
GO
