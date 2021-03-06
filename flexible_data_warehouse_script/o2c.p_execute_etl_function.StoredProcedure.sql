/****** Object:  StoredProcedure [o2c].[p_execute_etl_function]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_execute_etl_function] @imp_function nvarchar(max),
												@imp_tablename nvarchar(max),
												@schema varchar(max)
as

declare @table table
(
table_name varchar(max), 
column_name varchar(max),
parameter varchar(max),
id varchar(max)
)

declare @max int
declare @sql varchar(max) 
declare @tablename varchar(50)
declare @columnname varchar(50) 
declare @parameter varchar(max) 
declare @id int = 1

insert into @table(table_name, column_name, parameter, id)
select table_name, column_name, parameter, row_number() over (order by table_name, column_name desc) as id from config
where function_name = @imp_function and active = 'x' and table_name = @imp_tablename and db_schema = @schema

select @max = max(id) from @table

while (@id <= @max)
begin

	select @tablename = table_name, @columnname = column_name, @parameter = parameter from @table where id = @id

	if @imp_function = 'remove_zero' 
	begin
	set @sql =  'update '+@schema+'.'+@tablename+ ' set '+@columnname+' = substring('+@columnname+', patindex(''%[^0]%'', '+@columnname+'+''.''), len('+@columnname+'))'
	end 

	if @imp_function = 'add_zero' 
	begin 
	if @parameter = '10'
	begin
	set @sql = 'update '+@schema+'.'+@tablename+' set '+@columnname+' = right(''0000000000''+isnull('+@columnname+',''''),10)'
	end 
	if @parameter = '4'
	begin
	set @sql = 'update '+@schema+'.'+@tablename+' set '+@columnname+' = right(''0000''+isnull('+@columnname+',''''),4)' 
	end
	end 

	exec (@sql)

	set @id = @id +1
end

GO
