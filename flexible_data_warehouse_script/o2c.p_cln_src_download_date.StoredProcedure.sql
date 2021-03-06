/****** Object:  StoredProcedure [o2c].[p_cln_src_download_date]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_cln_src_download_date] @schema varchar(max) as

declare @table table
(
tablename varchar(50),
id int identity(1,1)
)

insert into @table
select table_name from information_schema.columns 
where column_name = 'file_path' and 
	  left(table_name,3) = 'cln' and 
	  table_name <> @schema+'.cln_load_details' and
	  table_schema = @schema


declare @max int
declare @sql varchar(max) 
declare @tablename varchar(50)
declare @id int = 1

select @max = max(id) from @table

while (@id <= @max)
begin

select @tablename = tablename from @table where id = @id

set @sql =	   'if col_length ('''+@schema+'.'+@tablename+''',''src_download_date'') is null
				begin
				alter table '+@schema+'.'+@tablename+' add src_download_date datetime
				end'

exec(@sql)
--print(@sql)
set @id = @id +1
end
GO
