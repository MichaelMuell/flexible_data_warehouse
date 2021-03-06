/****** Object:  StoredProcedure [o2c].[p_cln_first]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_first] @schema varchar(max) AS 

declare @table table
(
tablename varchar(50),
id int identity(1,1)
)

insert into @table
select distinct table_name from information_schema.columns 
where left(table_name,2) = 'in' and 
	 table_name <> 'ing_fi5000' and 
	 table_name <> 'ing_eflowtask' and
     table_schema = @schema

declare @max int
declare @sql varchar(max) 
declare @tablename varchar(50)
declare @id int = 1

select @max = max(id) from @table

while (@id <= @max)
begin

select @tablename = tablename from @table where id = @id
set @sql =     'drop table if exists '+@schema+'.cln'+substring(@tablename,4,20)+';
				select * into '+@schema+'.cln'+substring(@tablename,4,20)+' from '+@schema+'.'+@tablename+''

--print(@sql) 
exec(@sql)
set @id = @id +1
end


--recreate new cleaning LIKP table with index
	drop table if exists o2c.cln_likp 

	create table [o2c].[cln_likp](
		[delivery_nr] [nvarchar](10) not null primary key,
		[created_by] [nvarchar](40) null,
		[created_on] [datetime] null,
		[shippoint] [nvarchar](4)  null,
		[salesorg] [nvarchar](4) null,
		[delivery_type] [nvarchar](4) null,
		[delivery_date] [datetime] null,
		[billing_block] [nvarchar](10) null,
		[soldtoparty] [nvarchar](10) null,
		[rel_cre_value] [decimal](15, 2) null,
		[rel_cre_date] [datetime] null,
		[actual_gi_date] [datetime] null,
		[file_path] [nvarchar](100),
		[download_date] [datetime] null,
	) 

	 --delete unuseful raw data 
--DELETE from o2c.ing_eflowtask where PROCESSNAME not in ('P047_CLR_01','P048_GR_01')

drop table if exists o2c.cln_eflowtask

-- Create new cln eflow task table with index
create table o2c.cln_eflowtask (
	task_id nvarchar(50) not null,
	processname nvarchar(20) not null  ,
	incident nvarchar(10) not null  , 
	steplabel nvarchar(30), 
	taskuser nvarchar(40),
	 assignedtouser nvarchar(40),status int,
	 substatus int, 
	 starttime datetime, 
	 endtime datetime, 
	 download_date datetime
   )	

create index inx_eflowtask on o2c.cln_eflowtask( PROCESSNAME, INCIDENT)

--recreate new cln eflow dn table with index
drop table if exists o2c.cln_eflowdn 
create table o2c.cln_eflowdn (
	processname nvarchar(20) not null ,
	incident nvarchar(10) not null  , 
	dntbr nvarchar(20),
   download_date datetime)
create index inx_eflowdn on o2c.cln_eflowdn(processname,incident)
GO
