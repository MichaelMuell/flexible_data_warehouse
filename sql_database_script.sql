/****** Object:  Database [sdp-s-fssc]    Script Date: 4/8/2021 1:59:54 PM ******/
CREATE DATABASE [sdp-s-fssc]  (EDITION = 'Standard', SERVICE_OBJECTIVE = 'ElasticPool', MAXSIZE = 150 GB) WITH CATALOG_COLLATION = SQL_Latin1_General_CP1_CI_AS;
GO
ALTER DATABASE [sdp-s-fssc] SET COMPATIBILITY_LEVEL = 150
GO
ALTER DATABASE [sdp-s-fssc] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET ARITHABORT OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET AUTO_SHRINK ON 
GO
ALTER DATABASE [sdp-s-fssc] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [sdp-s-fssc] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [sdp-s-fssc] SET ALLOW_SNAPSHOT_ISOLATION ON 
GO
ALTER DATABASE [sdp-s-fssc] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [sdp-s-fssc] SET READ_COMMITTED_SNAPSHOT ON 
GO
ALTER DATABASE [sdp-s-fssc] SET  MULTI_USER 
GO
ALTER DATABASE [sdp-s-fssc] SET ENCRYPTION ON
GO
ALTER DATABASE [sdp-s-fssc] SET QUERY_STORE = ON
GO
ALTER DATABASE [sdp-s-fssc] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 100, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
/*** The scripts of database scoped configurations in Azure should be executed inside the target database connection. ***/
GO
-- ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 8;
GO
/****** Object:  User [sdp-s-d-fssc-df]    Script Date: 4/8/2021 1:59:54 PM ******/
CREATE USER [sdp-s-d-fssc-df] FROM  EXTERNAL PROVIDER  WITH DEFAULT_SCHEMA=[dbo]
GO
/****** Object:  User [RL_SDP_S_FSSC_DX_AAD]    Script Date: 4/8/2021 1:59:54 PM ******/
CREATE USER [RL_SDP_S_FSSC_DX_AAD] FROM  EXTERNAL PROVIDER 
GO
/****** Object:  DatabaseRole [db_developer]    Script Date: 4/8/2021 1:59:55 PM ******/
CREATE ROLE [db_developer]
GO
sys.sp_addrolemember @rolename = N'db_developer', @membername = N'sdp-s-d-fssc-df'
GO
sys.sp_addrolemember @rolename = N'db_developer', @membername = N'RL_SDP_S_FSSC_DX_AAD'
GO
sys.sp_addrolemember @rolename = N'db_owner', @membername = N'RL_SDP_S_FSSC_DX_AAD'
GO
/****** Object:  Schema [o2c]    Script Date: 4/8/2021 1:59:58 PM ******/
CREATE SCHEMA [o2c]
GO
/****** Object:  FullTextCatalog [fulltext_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
CREATE FULLTEXT CATALOG [fulltext_all_cust_items] WITH ACCENT_SENSITIVITY = ON
AS DEFAULT
GO
/****** Object:  UserDefinedFunction [dbo].[FC_CALCULATE_ARREARS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_CALCULATE_ARREARS]
(@start date, @end date)
RETURNS int
AS
BEGIN

DECLARE @arrears int

SET @arrears = DATEDIFF(day, @start, @end)

RETURN @arrears
END
GO
/****** Object:  UserDefinedFunction [dbo].[FC_CALCULATE_DOWNLOAD_DATE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_CALCULATE_DOWNLOAD_DATE]
(@path varchar)
RETURNS date
AS
BEGIN
DECLARE @duedate date

SET @duedate = CONVERT(date,LEFT(RIGHT(@path,30),8),112)

RETURN @duedate

END
GO
/****** Object:  UserDefinedFunction [dbo].[FC_CALCULATE_DUE_DATE_AP]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_CALCULATE_DUE_DATE_AP]
(@start date, @days1 int, @days2 int, @debit_credit varchar(Max), @follow_on_doc varchar(Max))
RETURNS date
AS
BEGIN

DECLARE @duedate date

IF @debit_credit = 'S' AND @follow_on_doc = ''
	BEGIN
	SET @duedate = @start 
	END 
ELSE 
	BEGIN
	IF @days2 = 0 
		BEGIN 
		SET @duedate = DATEADD(day,@days1,@start) 
		END 
	ELSE
		BEGIN
		SET @duedate = DATEADD(day, @days2, @start)
		END 
END

RETURN @duedate 

END
GO
/****** Object:  UserDefinedFunction [dbo].[FC_GET_BUSINESS_DAYS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FC_GET_BUSINESS_DAYS]
(@from datetime, @to datetime)
RETURNS int
AS
BEGIN

DECLARE @days int

IF @from = '' OR @to = ''
BEGIN  
	SET @days = 0
END 
ELSE 
BEGIN 
	SELECT @days = count(*)+1
	from CLN_PAYMENT_CALENDAR 
	where datepart(dw, DATES) not in (1,7)   
	and CHINA_PUBLIC_HOLIDAY IS NULL
	and DATES > @from and DATES <= @to
END

return ( @days )
END 
GO
/****** Object:  UserDefinedFunction [dbo].[FC_GET_SCAN_DATE_GERMAN]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_GET_SCAN_DATE_GERMAN]
(@date date, @hour int)
RETURNS date
AS
BEGIN 

IF @hour <6 
BEGIN 
SET @date = DATEADD(DAY, -1,@date)
END 

RETURN(@date)
END 
GO
/****** Object:  UserDefinedFunction [dbo].[fn_diagramobjects]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE FUNCTION [dbo].[fn_diagramobjects]() 
	RETURNS int
	WITH EXECUTE AS N'dbo'
	AS
	BEGIN
		declare @id_upgraddiagrams		int
		declare @id_sysdiagrams			int
		declare @id_helpdiagrams		int
		declare @id_helpdiagramdefinition	int
		declare @id_creatediagram	int
		declare @id_renamediagram	int
		declare @id_alterdiagram 	int 
		declare @id_dropdiagram		int
		declare @InstalledObjects	int

		select @InstalledObjects = 0

		select 	@id_upgraddiagrams = object_id(N'dbo.sp_upgraddiagrams'),
			@id_sysdiagrams = object_id(N'dbo.sysdiagrams'),
			@id_helpdiagrams = object_id(N'dbo.sp_helpdiagrams'),
			@id_helpdiagramdefinition = object_id(N'dbo.sp_helpdiagramdefinition'),
			@id_creatediagram = object_id(N'dbo.sp_creatediagram'),
			@id_renamediagram = object_id(N'dbo.sp_renamediagram'),
			@id_alterdiagram = object_id(N'dbo.sp_alterdiagram'), 
			@id_dropdiagram = object_id(N'dbo.sp_dropdiagram')

		if @id_upgraddiagrams is not null
			select @InstalledObjects = @InstalledObjects + 1
		if @id_sysdiagrams is not null
			select @InstalledObjects = @InstalledObjects + 2
		if @id_helpdiagrams is not null
			select @InstalledObjects = @InstalledObjects + 4
		if @id_helpdiagramdefinition is not null
			select @InstalledObjects = @InstalledObjects + 8
		if @id_creatediagram is not null
			select @InstalledObjects = @InstalledObjects + 16
		if @id_renamediagram is not null
			select @InstalledObjects = @InstalledObjects + 32
		if @id_alterdiagram  is not null
			select @InstalledObjects = @InstalledObjects + 64
		if @id_dropdiagram is not null
			select @InstalledObjects = @InstalledObjects + 128
		
		return @InstalledObjects 
	END
	
GO
/****** Object:  UserDefinedFunction [o2c].[fc_cal_eflow_duration]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [o2c].[fc_cal_eflow_duration]
(@from datetime, @to datetime)
RETURNS int
AS
BEGIN

DECLARE @days int
declare @durationmin int
declare @starttime time
declare @endtime time

IF @from = '' OR @to = ''
	BEGIN  
		SET @days = 0
		set @durationmin = 0
	END 
ELSE 
	BEGIN 
		set @days = dbo.FC_GET_BUSINESS_DAYS(@from,@to) - 1 
		set  @starttime = cast(@from as time(0))
		set  @endtime = cast(@to as time(0))
	-- only consider working time from 08:30 to 17:30
		if @starttime >= cast('00:00:00' as time(0)) and  @starttime <= cast('08:30:00' as time(0)) 
		begin
			set @starttime = cast('08:30:00' as time(0))
		end
		if @endtime >= cast('00:00:00' as time(0)) and  @endtime <= cast('08:30:00' as time(0)) 
		begin
			set @endtime = cast('08:30:00' as time(0))
		end
	
	-- set max time to 17:30:00
		if @starttime >= cast('17:30:00' as time(0)) and  @starttime <= cast('23:59:59' as time(0)) 
		begin
			set @starttime = cast('17:30:00' as time(0))
		end
		if @endtime >= cast('17:30:00' as time(0)) and  @endtime <= cast('23:59:59' as time(0)) 
		begin
			set @endtime = cast('17:30:00' as time(0))
		end
	
	    set @durationmin = DATEDIFF(MINUTE,@starttime,@endtime) + @days * 540


	END

return ( @durationmin )
END 
GO
/****** Object:  UserDefinedFunction [o2c].[fc_calculate_days1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE function [o2c].[fc_calculate_days1]
(@payment_terms nvarchar(max), @baseline_date nvarchar(max))
returns int
as
begin

--declare @payment_terms as nvarchar(max)
--declare @baseline_date as nvarchar(max)
declare @days1 as int
declare @days1_fixed as int 
declare @due_date_special1 as int
declare @month_special1 as int 
declare @date as date 
declare @day_limit as int

--set @payment_terms = 'Z304'
--set @baseline_date = '2021-01-09 00:00:00.0000000'

set @day_limit = (select min(day_limit)
					from o2c.cln_t052 
					where payment_term = @payment_terms)

set @baseline_date = try_convert(date,@baseline_date)
set @days1 = 0

if @day_limit = 0
begin 
	set @days1_fixed = (select max(days1_fixed)
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @due_date_special1 = (
						select max(due_date_special1) 
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @month_special1 = (
						select max(month_special1)
						from o2c.cln_t052 
						where payment_term = @payment_terms)
end 
else 
begin
	set @days1_fixed = (select top 1 days1_fixed
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2) 
						order by day_limit)

	set @due_date_special1 = (
						select top 1 due_date_special1
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)
	
	set @month_special1 = (
						select top 1 month_special1
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)
end

set @days1 = @days1_fixed

if @due_date_special1 > 0 or @month_special1 > 0
begin
	set @date = @baseline_date

	if @days1_fixed > 0 
	begin
	    set @date =  dateadd(day,@days1_fixed,convert(date,@baseline_date))
	end 
--		date calculation 
	set @date = DATEADD(MONTH,@month_special1,@date)
	
	if @due_date_special1 = 31
	begin 
		set @due_date_special1 = right(EOMONTH(@date),2)
	end

	set @date = DATEFROMPARTS(left(@date,4),right(left(@date,7),2),@due_date_special1)

	if  @date < @baseline_date 
	begin	
		set	@baseline_date = @date 
	end 

	set @days1 = DATEDIFF(DAY,@baseline_date,@date)

end
return @days1
end


GO
/****** Object:  UserDefinedFunction [o2c].[fc_calculate_days2]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE function [o2c].[fc_calculate_days2]
(@payment_terms nvarchar(max), @baseline_date nvarchar(max))
returns int
as
begin

--declare @payment_terms as nvarchar(max)
--declare @baseline_date as nvarchar(max)
declare @days2 as int
declare @days2_fixed as int 
declare @due_date_special2 as int
declare @month_special2 as int 
declare @date as date 
declare @day_limit as int

--set @payment_terms = '3330'
--set @baseline_date = '2020-10-31 00:00:00.0000000'

set @day_limit = (select min(day_limit)
					from o2c.cln_t052 
					where payment_term = @payment_terms)

set @baseline_date = try_convert(date,@baseline_date)
set @days2 = 0

if @day_limit = 0
begin 
	set @days2_fixed = (select max(days2_fixed)
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @due_date_special2 = (
						select max(due_date_special2) 
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @month_special2 = (
						select max(month_special2)
						from o2c.cln_t052 
						where payment_term = @payment_terms)
end 
else 
begin
	set @days2_fixed = (select top 1 days2_fixed
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)

	set @due_date_special2 = (
						select top 1 due_date_special2 
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)

	set @month_special2 = (
						select top 1 month_special2
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)
end

set @days2 =  @days2_fixed

if @due_date_special2 > 0 or @month_special2 > 0
begin
	set @date = @baseline_date

	if @days2_fixed > 0 
	begin
	    set @date =  dateadd(day,@days2_fixed,convert(date,@baseline_date))
	end 
--		date calculation 
	set @date = DATEADD(MONTH,@month_special2,@date)
	
	if @due_date_special2 = 31
	begin 
		set @due_date_special2 = right(EOMONTH(@date),2)
	end

	set @date = DATEFROMPARTS(left(@date,4),right(left(@date,7),2),@due_date_special2)

	if  @date < @baseline_date 
	begin	
		set	@baseline_date = @date 
	end 

	set @days2 = DATEDIFF(DAY,@baseline_date,@date)

end
return @days2
end


GO
/****** Object:  UserDefinedFunction [o2c].[fc_calculate_due_date_ar]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [o2c].[fc_calculate_due_date_ar]
(@start date, @days1 int, @days2 int, @debit_credit varchar(Max), @follow_on_doc varchar(Max))
RETURNS date
AS
BEGIN

DECLARE @duedate date

IF @debit_credit = 'H' AND @follow_on_doc = ''
	BEGIN
	SET @duedate = @start 
	END 
ELSE 
	BEGIN
	IF @days2 = 0 
		BEGIN 
		SET @duedate = DATEADD(day,@days1,@start) 
		END 
	ELSE
		BEGIN
		SET @duedate = DATEADD(day, @days2, @start)
		END 
END

RETURN @duedate 

END
GO
/****** Object:  UserDefinedFunction [o2c].[fc_convert_currency]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE function [o2c].[fc_convert_currency]
(@curr1 nvarchar(max), @amount1 decimal(30,2), @curr2 nvarchar(max))
returns decimal(30,2)
as
begin

declare @amount2 decimal(30,2)

set @amount2 =  @amount1 / (select max(exchangerate) 
							from o2c.currency 
							where currency1 = @curr1 
									and 
								  currency2 = @curr2)

return @amount2
end
GO
/****** Object:  UserDefinedFunction [o2c].[ft_get_due_days]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [o2c].[ft_get_due_days]
(
@transaction_key varchar(max) 
) 
returns @ResultTable table
( 
days1 varchar(max), days2 varchar(max)
) AS BEGIN

        INSERT INTO @ResultTable (days1, days2)
		select currency,key_date 
		from o2c.tp3_all_cust_items where 
		transaction_key = @transaction_key
               
RETURN
END

GO
/****** Object:  UserDefinedFunction [o2c].[FT_GET_DBTABLESINFO]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		WANGYNH
-- Create date: 2021-03-10
-- Description:	get DB tables detail ifnormation
-- =============================================
CREATE FUNCTION [o2c].[FT_GET_DBTABLESINFO] 
(	
	-- Add the parameters for the function here
	--<@param1, sysname, @p1> <Data_Type_For_Param1, , int>, 
	--<@param2, sysname, @p2> <Data_Type_For_Param2, , char>
)
RETURNS TABLE 
AS
RETURN 
(

-- get table details
WITH agg AS
(   -- Get info for Tables, Indexed Views, etc
    SELECT  ps.[object_id] AS [ObjectID],
            ps.index_id AS [IndexID],
            NULL AS [ParentIndexID],
            NULL AS [PassThroughIndexName],
            NULL AS [PassThroughIndexType],
            SUM(ps.in_row_data_page_count) AS [InRowDataPageCount],
            SUM(ps.used_page_count) AS [UsedPageCount],
            SUM(ps.reserved_page_count) AS [ReservedPageCount],
            SUM(ps.row_count) AS [RowCount],
            SUM(ps.lob_used_page_count + ps.row_overflow_used_page_count)
                    AS [LobAndRowOverflowUsedPageCount]
    FROM    sys.dm_db_partition_stats ps
    GROUP BY    ps.[object_id],
                ps.[index_id]
    UNION ALL
    -- Get info for FullText indexes, XML indexes, Spatial indexes, etc
    SELECT  sit.[parent_id] AS [ObjectID],
            sit.[object_id] AS [IndexID],
            sit.[parent_minor_id] AS [ParentIndexID],
            sit.[name] AS [PassThroughIndexName],
            sit.[internal_type_desc] AS [PassThroughIndexType],
            0 AS [InRowDataPageCount],
            SUM(ps.used_page_count) AS [UsedPageCount],
            SUM(ps.reserved_page_count) AS [ReservedPageCount],
            0 AS [RowCount],
            0 AS [LobAndRowOverflowUsedPageCount]
    FROM    sys.dm_db_partition_stats ps
    INNER JOIN  sys.internal_tables sit
            ON  sit.[object_id] = ps.[object_id]
    WHERE   sit.internal_type IN
               (202, 204, 207, 211, 212, 213, 214, 215, 216, 221, 222, 236)
    GROUP BY    sit.[parent_id],
                sit.[object_id],
                sit.[parent_minor_id],
                sit.[name],
                sit.[internal_type_desc]
), spaceused AS
(
SELECT  agg.[ObjectID],
        agg.[IndexID],
        agg.[ParentIndexID],
        agg.[PassThroughIndexName],
        agg.[PassThroughIndexType],
        OBJECT_SCHEMA_NAME(agg.[ObjectID]) AS [SchemaName],
        OBJECT_NAME(agg.[ObjectID]) AS [TableName],
        SUM(CASE
                WHEN (agg.IndexID < 2) THEN agg.[RowCount]
                ELSE 0
            END) AS [Rows],
        SUM(agg.ReservedPageCount) * 8 AS [ReservedKB],
        SUM(agg.LobAndRowOverflowUsedPageCount +
            CASE
                WHEN (agg.IndexID < 2) THEN (agg.InRowDataPageCount)
                ELSE 0
            END) * 8 AS [DataKB],
        SUM(agg.UsedPageCount - agg.LobAndRowOverflowUsedPageCount -
            CASE
                WHEN (agg.IndexID < 2) THEN agg.InRowDataPageCount
                ELSE 0
            END) * 8 AS [IndexKB],
        SUM(agg.ReservedPageCount - agg.UsedPageCount) * 8 AS [UnusedKB],
        SUM(agg.UsedPageCount) * 8 AS [UsedKB]
FROM    agg
GROUP BY    agg.[ObjectID],
            agg.[IndexID],
            agg.[ParentIndexID],
            agg.[PassThroughIndexName],
            agg.[PassThroughIndexType],
            OBJECT_SCHEMA_NAME(agg.[ObjectID]),
            OBJECT_NAME(agg.[ObjectID])
)

SELECT sp.SchemaName,
       sp.TableName,
       sp.IndexID,
       CASE
         WHEN (sp.IndexID > 0) THEN COALESCE(si.[name], sp.[PassThroughIndexName])
         ELSE N'<Heap>'
       END AS [IndexName],
       sp.[PassThroughIndexName] AS [InternalTableName],
       sp.[Rows],
       sp.ReservedKB,
       (sp.ReservedKB / 1024.0 / 1024.0) AS [ReservedGB],
       sp.DataKB,
       (sp.DataKB / 1024.0 / 1024.0) AS [DataGB],
       sp.IndexKB,
       (sp.IndexKB / 1024.0 / 1024.0) AS [IndexGB],
       sp.UsedKB AS [UsedKB],
       (sp.UsedKB / 1024.0 / 1024.0) AS [UsedGB],
       sp.UnusedKB,
       (sp.UnusedKB / 1024.0 / 1024.0) AS [UnusedGB],
       so.[type_desc] AS [ObjectType],
       COALESCE(si.type_desc, sp.[PassThroughIndexType]) AS [IndexPrimaryType],
       sp.[PassThroughIndexType] AS [IndexSecondaryType],
       SCHEMA_ID(sp.[SchemaName]) AS [SchemaID],
       sp.ObjectID
       --,sp.ParentIndexID
FROM   spaceused sp
INNER JOIN sys.all_objects so -- in case "WHERE so.is_ms_shipped = 0" is removed
        ON so.[object_id] = sp.ObjectID
LEFT JOIN  sys.indexes si
       ON  si.[object_id] = sp.ObjectID
      AND  (si.[index_id] = sp.IndexID
         OR si.[index_id] = sp.[ParentIndexID])
WHERE so.is_ms_shipped = 0
)
GO
/****** Object:  Table [dbo].[CLN_ADRC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_ADRC](
	[ADDRESS_NUMBER] [nvarchar](max) NULL,
	[NATION] [nvarchar](max) NULL,
	[NAME1] [nvarchar](max) NULL,
	[NAME2] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_BKPF]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_BKPF](
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[FISCAL_YEAR] [decimal](4, 0) NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_BSAK]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_BSAK](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [nvarchar](max) NULL,
	[CLEARING_DATE] [nvarchar](max) NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_BSIK]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_BSIK](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_EBAN]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_EBAN](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_EKBE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_EKBE](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_EKBE_test]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_EKBE_test](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_EKKO]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_EKKO](
	[CLIENT] [nvarchar](max) NULL,
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_FI5000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_FI5000](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_LOAD_DETAILS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_LOAD_DETAILS](
	[TABLE_NAME] [nvarchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime2](7) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_OCRLOG]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_OCRLOG](
	[TIF_FILE] [nvarchar](max) NULL,
	[FSSC_LOCATION] [nvarchar](max) NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_OVERDUE_REASON]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_OVERDUE_REASON](
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[YEAR] [nvarchar](max) NULL,
	[LINE_ITEM] [nvarchar](max) NULL,
	[KEY_DATE] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_PAYMENT_CALENDAR]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_PAYMENT_CALENDAR](
	[DATES] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_REGUP]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_REGUP](
	[RUN_DATE] [date] NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[PAYMENT_DOCUMENT] [nvarchar](max) NULL,
	[XVORL] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[FISCAL_YEAR] [decimal](4, 0) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[PAYMENT_DOCUMENT_YEAR] [varchar](max) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_T001]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_T001](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_T001S]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_T001S](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_T024]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_T024](
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_TRADESHIFT_INVOICES]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_TRADESHIFT_INVOICES](
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[TS_REFERENCE] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CLN_VF_KRED]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_VF_KRED](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[VENDOR_NAME1] [nvarchar](max) NULL,
	[VENDOR_NAME2] [nvarchar](max) NULL,
	[ADDRESS_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[ACCOUNT_GROUP] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CONFIG]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CONFIG](
	[FUNCTION_NAME] [varchar](max) NULL,
	[TABLE_NAME] [varchar](max) NULL,
	[COLUMN_NAME] [varchar](max) NULL,
	[PARAMETER] [varchar](max) NULL,
	[ACTIVE] [varchar](max) NULL,
	[DB_SCHEMA] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_ADRC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_ADRC](
	[ADDRESS_NUMBER] [nvarchar](max) NULL,
	[NATION] [nvarchar](max) NULL,
	[NAME1] [nvarchar](max) NULL,
	[NAME2] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_BKPF]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_BKPF](
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[FISCAL_YEAR] [decimal](4, 0) NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_BSAK]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_BSAK](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [nvarchar](max) NULL,
	[CLEARING_DATE] [nvarchar](max) NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_BSIK]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_BSIK](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_EBAN]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_EBAN](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_EKBE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_EKBE](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_EKBE_test]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_EKBE_test](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_EKKO]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_EKKO](
	[CLIENT] [nvarchar](max) NULL,
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_FI5000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_FI5000](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_LOAD_DETAILS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_LOAD_DETAILS](
	[TABLE_NAME] [nvarchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime2](7) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_OCRLOG]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_OCRLOG](
	[TIF_FILE] [nvarchar](max) NULL,
	[FSSC_LOCATION] [nvarchar](max) NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_REGUP]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_REGUP](
	[RUN_DATE] [date] NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[PAYMENT_DOCUMENT] [nvarchar](max) NULL,
	[XVORL] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[FISCAL_YEAR] [decimal](4, 0) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_T001]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_T001](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_T001S]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_T001S](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_T024]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_T024](
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ING_VF_KRED]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_VF_KRED](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[VENDOR_NAME1] [nvarchar](max) NULL,
	[VENDOR_NAME2] [nvarchar](max) NULL,
	[ADDRESS_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[ACCOUNT_GROUP] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[INX_OVERDUE_REASON]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[INX_OVERDUE_REASON](
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[YEAR] [nvarchar](max) NULL,
	[LINE_ITEM] [nvarchar](max) NULL,
	[KEY_DATE] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[INX_PAYMENT_CALENDAR]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[INX_PAYMENT_CALENDAR](
	[DATES] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[INX_TRADESHIFT_INVOICES]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[INX_TRADESHIFT_INVOICES](
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[TS_REFERENCE] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[metadata]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[metadata](
	[tablename] [nvarchar](257) NULL,
	[rowscount] [bigint] NULL,
	[colcount] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_ALL_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_ALL_ITEMS](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[DUE_DATE] [varchar](max) NULL,
	[ARREARS_AFTER_NET] [varchar](max) NULL,
	[TRANSACTION_KEY] [varchar](max) NULL,
	[WHT] [varchar](max) NULL,
	[DUPLICATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_ALL_ITEMS_SCHEMA]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_ALL_ITEMS_SCHEMA](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[DUE_DATE] [varchar](max) NULL,
	[ARREARS_AFTER_NET] [varchar](max) NULL,
	[TRANSACTION_KEY] [varchar](max) NULL,
	[WHT] [varchar](max) NULL,
	[DUPLICATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_CLEARED_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_CLEARED_ITEMS](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [date] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[DUE_DATE] [varchar](max) NULL,
	[ARREARS_AFTER_NET] [varchar](max) NULL,
	[TRANSACTION_KEY] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_IRB_FULL]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_IRB_FULL](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](13, 3) NULL,
	[IR_QUANTITY] [decimal](13, 3) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[SCAN_DATE_TO_INPUT_DATE] [varchar](max) NULL,
	[INPUT_DATE_TO_POSTING_DATE] [varchar](max) NULL,
	[EIV_AUTOPOST] [varchar](max) NULL,
	[KEY_DATE] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_IRB_FULL_SCHEMA]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_IRB_FULL_SCHEMA](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](13, 3) NULL,
	[IR_QUANTITY] [decimal](13, 3) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[SCAN_DATE_TO_INPUT_DATE] [varchar](max) NULL,
	[INPUT_DATE_TO_POSTING_DATE] [varchar](max) NULL,
	[EIV_AUTOPOST] [varchar](max) NULL,
	[KEY_DATE] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_IRB_MONTHLY]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_IRB_MONTHLY](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [date] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](38, 3) NULL,
	[IR_QUANTITY] [decimal](38, 3) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[SCAN_DATE_TO_INPUT_DATE] [varchar](max) NULL,
	[INPUT_DATE_TO_POSTING_DATE] [varchar](max) NULL,
	[EIV_AUTOPOST] [varchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[AMOUNT_EUR] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_IRB_MONTHLY_SCHEMA]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_IRB_MONTHLY_SCHEMA](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [date] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](38, 3) NULL,
	[IR_QUANTITY] [decimal](38, 3) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[SCAN_DATE_TO_INPUT_DATE] [varchar](max) NULL,
	[INPUT_DATE_TO_POSTING_DATE] [varchar](max) NULL,
	[EIV_AUTOPOST] [varchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[AMOUNT_EUR] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_OPEN_ITEMS_MONTHLY]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_OPEN_ITEMS_MONTHLY](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[DUE_DATE] [varchar](max) NULL,
	[ARREARS_AFTER_NET] [varchar](max) NULL,
	[TRANSACTION_KEY] [varchar](max) NULL,
	[WHT] [varchar](max) NULL,
	[DUPLICATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STA_OPEN_ITEMS_MONTHLY_SCHEMA]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STA_OPEN_ITEMS_MONTHLY_SCHEMA](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[DUE_DATE] [varchar](max) NULL,
	[ARREARS_AFTER_NET] [varchar](max) NULL,
	[TRANSACTION_KEY] [varchar](max) NULL,
	[WHT] [varchar](max) NULL,
	[DUPLICATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[sysdiagrams]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[sysdiagrams](
	[name] [sysname] NOT NULL,
	[principal_id] [int] NOT NULL,
	[diagram_id] [int] IDENTITY(1,1) NOT NULL,
	[version] [int] NULL,
	[definition] [varbinary](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[diagram_id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [UK_principal_name] UNIQUE NONCLUSTERED 
(
	[principal_id] ASC,
	[name] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP1_EKBE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP1_EKBE](
	[MATERIAL_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](13, 3) NULL,
	[IR_QUANTITY] [decimal](13, 3) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP1_IRB]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP1_IRB](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP1_REGUP]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP1_REGUP](
	[RUN_DATE] [date] NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[PAYMENT_DOCUMENT] [nvarchar](max) NULL,
	[XVORL] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[PAYMENT_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP1_VENDOR_DIMENSION]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP1_VENDOR_DIMENSION](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NOT NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NOT NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP2_ALL_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_ALL_ITEMS](
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP2_CLEARED_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_CLEARED_ITEMS](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [nvarchar](max) NULL,
	[CLEARING_DATE] [nvarchar](max) NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[src_download_date] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP2_CLEARED_ITEMS_TEST]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_CLEARED_ITEMS_TEST](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [nvarchar](max) NULL,
	[CLEARING_DATE] [nvarchar](max) NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP2_EKBE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_EKBE](
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP2_IRB]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_IRB](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](13, 3) NULL,
	[IR_QUANTITY] [decimal](13, 3) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP2_OPEN_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_OPEN_ITEMS](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[src_download_date] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP3_ALL_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP3_ALL_ITEMS](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[CLEARING_DATE] [date] NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [date] NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[CLEARING_DOCUMENT_YEAR] [varchar](max) NULL,
	[src_download_date] [datetime] NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[CITY] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[RUN_DATE] [date] NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[DUE_DATE] [varchar](max) NULL,
	[ARREARS_AFTER_NET] [varchar](max) NULL,
	[TRANSACTION_KEY] [varchar](max) NULL,
	[WHT] [varchar](max) NULL,
	[DUPLICATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TP3_IRB]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP3_IRB](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[PO_COMPANY_CODE] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[COMPANY_NAME] [nvarchar](max) NULL,
	[COMPANY_NAME_SHORT] [varchar](max) NULL,
	[PURCHASER_NAME] [nvarchar](max) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](13, 3) NULL,
	[IR_QUANTITY] [decimal](13, 3) NULL,
	[SCAN_DATE_TO_INPUT_DATE] [varchar](max) NULL,
	[INPUT_DATE_TO_POSTING_DATE] [varchar](max) NULL,
	[EIV_AUTOPOST] [varchar](max) NULL,
	[KEY_DATE] [date] NULL,
	[AMOUNT_EUR] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_adrc]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_adrc](
	[address_number] [nvarchar](max) NULL,
	[nation] [nvarchar](max) NULL,
	[name1] [nvarchar](max) NULL,
	[name2] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_bkpf]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_bkpf](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_bsad]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_bsad](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [date] NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [date] NULL,
	[document_date] [date] NULL,
	[entry_date] [date] NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [date] NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [date] NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_bsid]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_bsid](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_eflowdn]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_eflowdn](
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[dntbr] [nvarchar](20) NULL,
	[download_date] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_eflowtask]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_eflowtask](
	[task_id] [nvarchar](50) NOT NULL,
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[steplabel] [nvarchar](30) NULL,
	[taskuser] [nvarchar](40) NULL,
	[assignedtouser] [nvarchar](40) NULL,
	[status] [int] NULL,
	[substatus] [int] NULL,
	[starttime] [datetime] NULL,
	[endtime] [datetime] NULL,
	[download_date] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_fdm_dcproc]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_fdm_dcproc](
	[dispute_id] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_fi1000](
	[company_code] [nvarchar](255) NULL,
	[customer_number] [nvarchar](255) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[credit_control_area] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[aramount] [float] NOT NULL,
	[salesamount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_kna1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_kna1](
	[customer_number] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[address_number] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[account_group] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_knb1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_knb1](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_knkk]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_knkk](
	[customer_number] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[block_indicator] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_likp]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_likp](
	[delivery_nr] [nvarchar](10) NOT NULL,
	[created_by] [nvarchar](40) NULL,
	[created_on] [datetime] NULL,
	[shippoint] [nvarchar](4) NULL,
	[salesorg] [nvarchar](4) NULL,
	[delivery_type] [nvarchar](4) NULL,
	[delivery_date] [datetime] NULL,
	[billing_block] [nvarchar](10) NULL,
	[soldtoparty] [nvarchar](10) NULL,
	[rel_cre_value] [decimal](15, 2) NULL,
	[rel_cre_date] [datetime] NULL,
	[actual_gi_date] [datetime] NULL,
	[file_path] [nvarchar](100) NULL,
	[download_date] [datetime] NULL,
	[src_download_date] [datetime] NULL,
	[durationmin_task_id] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[delivery_nr] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_load_details]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_load_details](
	[table_name] [nvarchar](max) NULL,
	[src_download_date] [datetime2](7) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_sample_orders]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_sample_orders](
	[document_number] [nvarchar](max) NULL,
	[company_code] [nvarchar](max) NULL,
	[year] [nvarchar](max) NULL,
	[line_item] [nvarchar](max) NULL,
	[key_date] [nvarchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_scmg_t_case_attr]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_scmg_t_case_attr](
	[dispute_id] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_t001]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_t001](
	[company_code] [nvarchar](max) NULL,
	[company_name] [nvarchar](max) NULL,
	[city] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_t001s]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_t001s](
	[company_code] [nvarchar](max) NULL,
	[accounting_clerk_number] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_t052]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_t052](
	[payment_term] [nvarchar](max) NULL,
	[day_limit] [decimal](2, 0) NULL,
	[date_type] [nvarchar](max) NULL,
	[calendar_day_for_baseline_date_of_payment] [decimal](2, 0) NULL,
	[additional_months] [decimal](2, 0) NULL,
	[days1_fixed] [decimal](3, 0) NULL,
	[days2_fixed] [decimal](3, 0) NULL,
	[days3_fixed] [decimal](3, 0) NULL,
	[due_date_special1] [decimal](2, 0) NULL,
	[month_special1] [decimal](2, 0) NULL,
	[file_path] [nvarchar](max) NULL,
	[due_date_special2] [decimal](2, 0) NULL,
	[month_special2] [decimal](2, 0) NULL,
	[due_date_special3] [decimal](2, 0) NULL,
	[month_special3] [decimal](2, 0) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_udmcaseattr00]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_udmcaseattr00](
	[dispute_id] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[cln_vbuk]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_vbuk](
	[delivery_nr] [nvarchar](10) NOT NULL,
	[delivery_status] [nvarchar](1) NULL,
	[gi_status] [nvarchar](1) NULL,
	[billing_status] [nvarchar](1) NULL,
	[file_path] [nvarchar](100) NULL,
	[download_date] [datetime] NULL,
	[src_download_date] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[delivery_nr] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[currency]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[currency](
	[currency1] [nvarchar](10) NULL,
	[currency2] [nvarchar](10) NULL,
	[exchangerate] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_adrc]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_adrc](
	[address_number] [nvarchar](max) NULL,
	[nation] [nvarchar](max) NULL,
	[name1] [nvarchar](max) NULL,
	[name2] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_bkpf]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_bkpf](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_bsad]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_bsad](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [date] NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [date] NULL,
	[document_date] [date] NULL,
	[entry_date] [date] NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [date] NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [date] NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_bsid]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_bsid](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_eflowdn]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_eflowdn](
	[processname] [nvarchar](max) NULL,
	[incident] [int] NULL,
	[dntbr] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_eflowtask]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_eflowtask](
	[task_id] [nvarchar](max) NULL,
	[processname] [nvarchar](max) NULL,
	[incident] [int] NULL,
	[steplabel] [nvarchar](max) NULL,
	[taskuser] [nvarchar](max) NULL,
	[assignedtouser] [nvarchar](max) NULL,
	[status] [int] NULL,
	[substatus] [int] NULL,
	[starttime] [datetime2](7) NULL,
	[endtime] [datetime2](7) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_fdm_dcproc]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_fdm_dcproc](
	[dispute_id] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_fi1000](
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[company_code] [nvarchar](255) NULL,
	[credit_control_area] [nvarchar](255) NULL,
	[customer_number] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[ar_month_0] [float] NULL,
	[ar_month_1] [float] NULL,
	[ar_month_2] [float] NULL,
	[ar_month_3] [float] NULL,
	[ar_month_4] [float] NULL,
	[ar_month_5] [float] NULL,
	[ar_month_6] [float] NULL,
	[ar_month_7] [float] NULL,
	[ar_month_8] [float] NULL,
	[ar_month_9] [float] NULL,
	[ar_month_10] [float] NULL,
	[ar_month_11] [float] NULL,
	[ar_month_12] [float] NULL,
	[sales_month_0] [float] NULL,
	[sales_month_1] [float] NULL,
	[sales_month_2] [float] NULL,
	[sales_month_3] [float] NULL,
	[sales_month_4] [float] NULL,
	[sales_month_5] [float] NULL,
	[sales_month_6] [float] NULL,
	[sales_month_7] [float] NULL,
	[sales_month_8] [float] NULL,
	[sales_month_9] [float] NULL,
	[sales_month_10] [float] NULL,
	[sales_month_11] [float] NULL,
	[sales_month_12] [float] NULL,
	[overdue_month_0] [float] NULL,
	[overdue_month_1] [float] NULL,
	[overdue_month_2] [float] NULL,
	[overdue_month_3] [float] NULL,
	[overdue_month_4] [float] NULL,
	[overdue_month_5] [float] NULL,
	[overdue_month_6] [float] NULL,
	[overdue_month_7] [float] NULL,
	[overdue_month_8] [float] NULL,
	[overdue_month_9] [float] NULL,
	[overdue_month_10] [float] NULL,
	[overdue_month_11] [float] NULL,
	[overdue_month_12] [float] NULL,
	[monthyearfrom] [nvarchar](255) NULL,
	[src_download_date] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_kna1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_kna1](
	[customer_number] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[address_number] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[account_group] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_knb1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_knb1](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_knkk]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_knkk](
	[customer_number] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[block_indicator] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_likp]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_likp](
	[delivery_nr] [nvarchar](max) NULL,
	[created_by] [nvarchar](max) NULL,
	[created_on] [datetime2](7) NULL,
	[shippoint] [nvarchar](max) NULL,
	[salesorg] [nvarchar](max) NULL,
	[delivery_type] [nvarchar](max) NULL,
	[delivery_date] [datetime2](7) NULL,
	[billing_block] [nvarchar](max) NULL,
	[soldtoparty] [nvarchar](max) NULL,
	[rel_cre_value] [decimal](15, 2) NULL,
	[rel_cre_date] [datetime2](7) NULL,
	[actual_gi_date] [datetime2](7) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_load_details]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_load_details](
	[table_name] [nvarchar](max) NULL,
	[src_download_date] [datetime2](7) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_scmg_t_case_attr]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_scmg_t_case_attr](
	[dispute_id] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_t001]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_t001](
	[company_code] [nvarchar](max) NULL,
	[company_name] [nvarchar](max) NULL,
	[city] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_t001s]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_t001s](
	[company_code] [nvarchar](max) NULL,
	[accounting_clerk_number] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_t052]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_t052](
	[payment_term] [nvarchar](max) NULL,
	[day_limit] [decimal](2, 0) NULL,
	[date_type] [nvarchar](max) NULL,
	[calendar_day_for_baseline_date_of_payment] [decimal](2, 0) NULL,
	[additional_months] [decimal](2, 0) NULL,
	[days1_fixed] [decimal](3, 0) NULL,
	[days2_fixed] [decimal](3, 0) NULL,
	[days3_fixed] [decimal](3, 0) NULL,
	[due_date_special1] [decimal](2, 0) NULL,
	[month_special1] [decimal](2, 0) NULL,
	[file_path] [nvarchar](max) NULL,
	[due_date_special2] [decimal](2, 0) NULL,
	[month_special2] [decimal](2, 0) NULL,
	[due_date_special3] [decimal](2, 0) NULL,
	[month_special3] [decimal](2, 0) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_udmcaseattr00]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_udmcaseattr00](
	[dispute_id] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[ing_vbuk]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_vbuk](
	[delivery_nr] [nvarchar](max) NULL,
	[delivery_status] [nvarchar](max) NULL,
	[gi_status] [nvarchar](max) NULL,
	[billing_status] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[inx_sample_orders]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[inx_sample_orders](
	[document_number] [nvarchar](max) NULL,
	[company_code] [nvarchar](max) NULL,
	[year] [nvarchar](max) NULL,
	[line_item] [nvarchar](max) NULL,
	[key_date] [nvarchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_all_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](50) NOT NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[key_date] [date] NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [int] NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [int] NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[days1_vat] [int] NULL,
	[days2_vat] [int] NULL,
	[reverse_document] [nvarchar](max) NULL,
	[overdue_rank_vat] [varchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_all_cust_items_schema]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_all_cust_items_schema](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](50) NOT NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[key_date] [date] NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [int] NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [int] NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[days1_vat] [int] NULL,
	[days2_vat] [int] NULL,
	[reverse_document] [nvarchar](max) NULL,
	[overdue_rank_vat] [varchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_eflow_clr]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_eflow_clr](
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[steplabel] [nvarchar](30) NULL,
	[status] [int] NULL,
	[StatusText] [varchar](8) NULL,
	[substatus] [int] NULL,
	[taskuser] [nvarchar](40) NULL,
	[assignedtouser] [nvarchar](40) NULL,
	[starttime] [datetime] NULL,
	[endtime] [datetime] NULL,
	[task_id] [nvarchar](50) NOT NULL,
	[durationmin] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_eflow_clr_schema]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_eflow_clr_schema](
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[steplabel] [nvarchar](30) NULL,
	[status] [int] NULL,
	[StatusText] [varchar](8) NULL,
	[substatus] [int] NULL,
	[taskuser] [nvarchar](40) NULL,
	[assignedtouser] [nvarchar](40) NULL,
	[starttime] [datetime] NULL,
	[endtime] [datetime] NULL,
	[task_id] [nvarchar](50) NOT NULL,
	[durationmin] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_eflow_likp]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_eflow_likp](
	[delivery_nr] [nvarchar](10) NOT NULL,
	[created_by] [nvarchar](40) NULL,
	[created_on] [datetime] NULL,
	[shippoint] [nvarchar](4) NULL,
	[salesorg] [nvarchar](4) NULL,
	[delivery_type] [nvarchar](4) NULL,
	[delivery_date] [datetime] NULL,
	[billing_block] [nvarchar](10) NULL,
	[soldtoparty] [nvarchar](10) NULL,
	[rel_cre_value] [decimal](15, 2) NULL,
	[rel_cre_date] [datetime] NULL,
	[actual_gi_date] [datetime] NULL,
	[file_path] [nvarchar](100) NULL,
	[download_date] [datetime] NULL,
	[src_download_date] [datetime] NULL,
	[durationmin_task_id] [int] NULL,
	[customer_country] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[delivery_status] [nvarchar](max) NULL,
	[gi_status] [nvarchar](max) NULL,
	[billing_status] [nvarchar](max) NULL,
	[status] [int] NULL,
	[statustext] [varchar](8) NULL,
	[substatus] [int] NULL,
	[task_id] [nvarchar](50) NULL,
	[durationmin] [int] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_eflow_likp_schema]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_eflow_likp_schema](
	[delivery_nr] [nvarchar](10) NOT NULL,
	[created_by] [nvarchar](40) NULL,
	[created_on] [datetime] NULL,
	[shippoint] [nvarchar](4) NULL,
	[salesorg] [nvarchar](4) NULL,
	[delivery_type] [nvarchar](4) NULL,
	[delivery_date] [datetime] NULL,
	[billing_block] [nvarchar](10) NULL,
	[soldtoparty] [nvarchar](10) NULL,
	[rel_cre_value] [decimal](15, 2) NULL,
	[rel_cre_date] [datetime] NULL,
	[actual_gi_date] [datetime] NULL,
	[file_path] [nvarchar](100) NULL,
	[download_date] [datetime] NULL,
	[src_download_date] [datetime] NULL,
	[durationmin_task_id] [int] NULL,
	[customer_country] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[delivery_status] [nvarchar](max) NULL,
	[gi_status] [nvarchar](max) NULL,
	[billing_status] [nvarchar](max) NULL,
	[status] [int] NULL,
	[statustext] [varchar](8) NULL,
	[substatus] [int] NULL,
	[task_id] [nvarchar](50) NULL,
	[durationmin] [int] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_fi1000](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[salesamount] [float] NOT NULL,
	[aramount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL,
	[payment_behavior_ar_amount] [float] NULL,
	[sum_overdue_0_30] [float] NULL,
	[sum_overdue_30_60] [float] NULL,
	[sum_overdue_60] [float] NULL,
	[receiveables_0_30] [varchar](max) NULL,
	[receiveables_30_60] [varchar](max) NULL,
	[receiveables_60] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_fi1000_schema]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_fi1000_schema](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[salesamount] [float] NOT NULL,
	[aramount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL,
	[payment_behavior_ar_amount] [float] NULL,
	[sum_overdue_0_30] [float] NULL,
	[sum_overdue_30_60] [float] NULL,
	[sum_overdue_60] [float] NULL,
	[receiveables_0_30] [varchar](max) NULL,
	[receiveables_30_60] [varchar](max) NULL,
	[receiveables_60] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_open_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_open_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](50) NOT NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[key_date] [date] NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [int] NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [int] NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[days1_vat] [int] NULL,
	[days2_vat] [int] NULL,
	[reverse_document] [nvarchar](max) NULL,
	[overdue_rank_vat] [varchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_open_cust_items_backup]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_open_cust_items_backup](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[key_date] [date] NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [varchar](max) NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [varchar](max) NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[amount_document] [varchar](max) NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[reason] [varchar](max) NULL,
	[reason_details] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_open_cust_items_schema]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_open_cust_items_schema](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](50) NOT NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[key_date] [date] NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [int] NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [int] NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[days1_vat] [int] NULL,
	[days2_vat] [int] NULL,
	[reverse_document] [nvarchar](max) NULL,
	[overdue_rank_vat] [varchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_payment_behavior]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_payment_behavior](
	[credit_account] [nvarchar](max) NULL,
	[key_date] [date] NULL,
	[1-30] [float] NULL,
	[31-90] [float] NULL,
	[90+] [float] NULL,
	[not_due] [float] NULL,
	[sales_by_ca] [float] NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[sta_payment_behavior_schema]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_payment_behavior_schema](
	[credit_account] [nvarchar](max) NULL,
	[key_date] [date] NULL,
	[1-30] [float] NULL,
	[31-90] [float] NULL,
	[90+] [float] NULL,
	[not_due] [float] NULL,
	[sales_by_ca] [float] NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[test_data]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[test_data](
	[prim_key] [nvarchar](max) NOT NULL,
	[customer_number] [nvarchar](max) NULL,
	[company_code] [nvarchar](max) NULL,
	[credit_account] [nvarchar](max) NULL,
	[reportdate] [date] NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[receiveables_amount] [float] NULL,
	[0-30] [float] NULL,
	[60+] [float] NULL,
	[30-60] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp1_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp1_all_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp1_customer]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp1_customer](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp1_dispute]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp1_dispute](
	[dispute_id] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp2_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp2_all_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[sample_order] [nvarchar](max) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp2_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp2_fi1000](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[salesamount] [float] NOT NULL,
	[aramount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp3_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp3_all_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](50) NOT NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[reverse_document] [nvarchar](max) NULL,
	[sample_order] [nvarchar](max) NULL,
	[key_date] [date] NULL,
	[company_code_currency] [varchar](max) NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [int] NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [int] NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[days1_vat] [int] NULL,
	[days2_vat] [int] NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[overdue_rank_vat] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[transaction_key] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp3_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp3_fi1000](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[salesamount] [float] NOT NULL,
	[aramount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL,
	[payment_behavior_ar_amount] [float] NULL,
	[sum_overdue_0_30] [float] NULL,
	[sum_overdue_30_60] [float] NULL,
	[sum_overdue_60] [float] NULL,
	[receiveables_0_30] [varchar](max) NULL,
	[receiveables_30_60] [varchar](max) NULL,
	[receiveables_60] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [o2c].[tp3_payment_behavior]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp3_payment_behavior](
	[credit_account] [nvarchar](max) NULL,
	[year] [int] NULL,
	[month] [int] NULL,
	[postdate] [date] NULL,
	[0-30] [float] NULL,
	[60+] [float] NULL,
	[30-60] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [inx_eflowdn]    Script Date: 4/8/2021 1:59:59 PM ******/
CREATE NONCLUSTERED INDEX [inx_eflowdn] ON [o2c].[cln_eflowdn]
(
	[processname] ASC,
	[incident] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [inx_eflowtask]    Script Date: 4/8/2021 1:59:59 PM ******/
CREATE NONCLUSTERED INDEX [inx_eflowtask] ON [o2c].[cln_eflowtask]
(
	[processname] ASC,
	[incident] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [i1]    Script Date: 4/8/2021 1:59:59 PM ******/
CREATE UNIQUE NONCLUSTERED INDEX [i1] ON [o2c].[tp3_all_cust_items]
(
	[transaction_key] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
/****** Object:  Index [i2]    Script Date: 4/8/2021 1:59:59 PM ******/
CREATE NONCLUSTERED INDEX [i2] ON [o2c].[tp3_all_cust_items]
(
	[arrears_after_net] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
/****** Object:  Index [i3]    Script Date: 4/8/2021 1:59:59 PM ******/
CREATE NONCLUSTERED INDEX [i3] ON [o2c].[tp3_all_cust_items]
(
	[arrears_after_net_vat] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_ADRC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_ADRC] AS 

WITH adrc_duplicates AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                ADDRESS_NUMBER 
		    ORDER BY 
		        ADDRESS_NUMBER  
        ) row_num
     FROM 
        CLN_ADRC
)

DELETE FROM adrc_duplicates
WHERE row_num > 1

UPDATE CLN_ADRC
SET CLN_ADRC.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'ADRC'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_BKPF]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_BKPF] AS

WITH BKPF_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COMPANY_CODE, 
				DOCUMENT_NUMBER,
				FISCAL_YEAR 
		    ORDER BY 
		        COMPANY_CODE,
				DOCUMENT_NUMBER,
				FISCAL_YEAR
        ) ROW_NUM
     FROM 
        CLN_BKPF
)

DELETE FROM BKPF_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_BKPF
SET CLN_BKPF.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'BKPF_FSSC_IMPROVEMENT_FRAMEWORK'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_BSAK]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_BSAK] AS 

-- DEDUPLICATE PRIMARY KEYS 

WITH BSAK_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COMPANY_CODE, 
                DOCUMENT_NUMBER, 
				[YEAR], 
				LINE_ITEM
		    ORDER BY 
		        COMPANY_CODE, 
                DOCUMENT_NUMBER, 
				[YEAR], 
				LINE_ITEM
        ) ROW_NUM
     FROM 
        CLN_BSAK
)

DELETE FROM BSAK_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_BSAK
SET CLN_BSAK.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'BSAK'

UPDATE CLN_BSAK SET CLEARING_DOCUMENT_YEAR = RIGHT(LEFT(CLEARING_DATE,10),4);
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_BSIK]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_BSIK] AS

WITH BSIK_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COMPANY_CODE,
				DOCUMENT_NUMBER,
				[YEAR], 
				LINE_ITEM
		    ORDER BY 
                COMPANY_CODE,
				DOCUMENT_NUMBER,
				[YEAR], 
				LINE_ITEM
        ) ROW_NUM
     FROM 
        CLN_BSIK
)

DELETE FROM BSIK_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_BSIK
SET CLN_BSIK.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'BSIK'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_EBAN]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_EBAN] AS 

WITH EBAN_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                PURCHASE_ORDER
		    ORDER BY 
		        PURCHASE_ORDER
        ) ROW_NUM
     FROM 
        CLN_EBAN
)

DELETE FROM EBAN_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_EBAN
SET CLN_EBAN.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'EBAN'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_EKBE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_EKBE] AS

GO
/****** Object:  StoredProcedure [dbo].[P_CLN_EKKO]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_EKKO] AS

WITH EKKO_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                PURCHASE_ORDER
            ORDER BY 
				PURCHASE_ORDER
        ) ROW_NUM
     FROM 
        CLN_EKKO
)

DELETE FROM EKKO_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_EKKO
SET CLN_EKKO.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'EKKO'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_EXEC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_EXEC] AS

-- FIRST---------------------------------------------------------------------

EXEC P_CLN_FIRST

EXEC o2c.P_CLN_SRC_DOWNLOAD_DATE @schema = 'dbo'

EXEC P_CLN_LOAD_DETAILS
----------------------------------------------------------------------------

EXEC P_CLN_ADRC 

EXEC P_CLN_BKPF

EXEC P_CLN_BSIK

EXEC P_CLN_EBAN

EXEC P_CLN_EKBE

EXEC P_CLN_EKKO

EXEC P_CLN_OVERDUE_REASON

EXEC P_CLN_PAYMENT_CALENDAR

EXEC P_CLN_T001S

EXEC P_CLN_T024 

EXEC P_CLN_VF_KRED

EXEC P_CLN_OCRLOG

EXEC P_CLN_T001

EXEC P_CLN_BSAK

EXEC P_CLN_REGUP

EXEC P_CLN_TS_INVOICES

--LAST   ---------------------------------------------------------------------------

EXEC o2c.P_CLN_CLEAN_COLUMNS @schema  = 'dbo'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_FIRST]    Script Date: 4/8/2021 1:59:59 PM ******/
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
/****** Object:  StoredProcedure [dbo].[P_CLN_LOAD_DETAILS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_LOAD_DETAILS] AS

WITH LOAD_DETAILS_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
				TABLE_NAME
		    ORDER BY 
				TABLE_NAME,
                DOWNLOAD_DATE DESC
        ) ROW_NUM
     FROM 
        CLN_LOAD_DETAILS
)

DELETE FROM LOAD_DETAILS_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_LOAD_DETAILS SET TABLE_NAME = SUBSTRING(TABLE_NAME,5,LEN(TABLE_NAME))
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_OCRLOG]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_OCRLOG] AS

WITH ocrlog_duplicates AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                TIF_FILE
            ORDER BY 
				TIF_FILE
        ) row_num
     FROM 
        CLN_OCRLOG
)

DELETE FROM ocrlog_duplicates
WHERE row_num > 1

UPDATE CLN_OCRLOG
SET CLN_OCRLOG.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'ZSI_IR_IC_OCRLOG'

UPDATE CLN_OCRLOG SET TIF_FILE = LEFT(UPPER(TIF_FILE),40)
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_OVERDUE_REASON]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_OVERDUE_REASON] AS

EXEC o2c.P_EXECUTE_ETL_FUNCTION @IMP_FUNCTION = 'ADD_ZERO' , @IMP_TABLENAME = 'CLN_OVERDUE_REASON', @schema = 'dbo';

WITH OVERDUE_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                DOCUMENT_NUMBER,
				COMPANY_CODE, 
				[YEAR], 
				LINE_ITEM
            ORDER BY 
				DOCUMENT_NUMBER,
				COMPANY_CODE, 
				[YEAR], 
				LINE_ITEM, 
				KEY_DATE DESC
        ) ROW_NUM
     FROM 
        CLN_OVERDUE_REASON
)

DELETE FROM OVERDUE_DUPLICATES
WHERE ROW_NUM > 1 
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_PAYMENT_CALENDAR]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_PAYMENT_CALENDAR] AS

WITH CALENDAR_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                DATES
            ORDER BY 
				DATES
        ) ROW_NUM
     FROM 
        CLN_PAYMENT_CALENDAR
)

DELETE FROM CALENDAR_DUPLICATES
WHERE ROW_NUM > 1
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_REGUP]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_REGUP] AS

UPDATE CLN_REGUP SET PAYMENT_DOCUMENT_YEAR = LEFT(RUN_DATE,4);

WITH REGUP_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                PAYMENT_DOCUMENT_YEAR, 
				COMPANY_CODE, 
				PAYMENT_DOCUMENT
            ORDER BY 
				PAYMENT_DOCUMENT_YEAR, 
				COMPANY_CODE, 
				PAYMENT_DOCUMENT 
        ) ROW_NUM
     FROM 
        CLN_REGUP
)

DELETE FROM REGUP_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_REGUP
SET CLN_REGUP.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'REGUP_FSSC_IMPROVEMENT_FRAMEWORK'

GO
/****** Object:  StoredProcedure [dbo].[P_CLN_T001]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_T001] AS

-- DEDUPLICATE PRIMARY KEYS 

WITH T001_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COMPANY_CODE 
		    ORDER BY 
		        COMPANY_CODE 
        ) ROW_NUM
     FROM 
        CLN_T001
)

DELETE FROM T001_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'IZT'  WHERE COMPANY_CODE = '0083' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'SAM'  WHERE COMPANY_CODE = '0189' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'STS'  WHERE COMPANY_CODE = '0199' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'FXR'  WHERE COMPANY_CODE = '0289' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'LFC'  WHERE COMPANY_CODE = '0369' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'SAB'  WHERE COMPANY_CODE = '0371' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'SNJ'  WHERE COMPANY_CODE = '0377' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'SXT'  WHERE COMPANY_CODE = '0404' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'SIC'  WHERE COMPANY_CODE = '0426' 
UPDATE CLN_T001 SET COMPANY_NAME_SHORT = 'STE'  WHERE COMPANY_CODE = '0429' 

UPDATE CLN_T001
SET CLN_T001.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'T001'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_T001S]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_T001S] AS

WITH T001S_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COMPANY_CODE, 
				ACCOUNTING_CLERK_NUMBER
            ORDER BY 
			    COMPANY_CODE, 
				ACCOUNTING_CLERK_NUMBER
        ) ROW_NUM
     FROM 
        CLN_T001S
)

DELETE FROM T001S_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_T001S
SET CLN_T001S.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'T001S'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_T024]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_T024] AS

WITH T024_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
				PURCHASING_GROUP
            ORDER BY 
				PURCHASING_GROUP
        ) ROW_NUM
     FROM 
        CLN_T024
)

DELETE FROM T024_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_T024
SET CLN_T024.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'T024'
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_TS_INVOICES]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_TS_INVOICES] AS

WITH TS_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                TS_FAPIAO_CODE, 
                TS_REFERENCE
            ORDER BY 
                TS_FAPIAO_CODE,
				TS_REFERENCE
        ) ROW_NUM
     FROM 
        CLN_TRADESHIFT_INVOICES
)

DELETE FROM TS_DUPLICATES
WHERE ROW_NUM > 1
GO
/****** Object:  StoredProcedure [dbo].[P_CLN_VF_KRED]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_VF_KRED] AS

WITH VF_KRED_DUPLICATES AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
				COMPANY_CODE, 
				VENDOR_NUMBER
            ORDER BY 
				COMPANY_CODE, 
				VENDOR_NUMBER 
        ) ROW_NUM
     FROM 
        CLN_VF_KRED
)

DELETE FROM VF_KRED_DUPLICATES
WHERE ROW_NUM > 1

UPDATE CLN_VF_KRED
SET CLN_VF_KRED.SRC_DOWNLOAD_DATE  = SRC.SRC_DOWNLOAD_DATE
FROM CLN_LOAD_DETAILS AS SRC
WHERE TABLE_NAME = 'VF_KRED'
GO
/****** Object:  StoredProcedure [dbo].[P_STA_ALL_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_STA_ALL_ITEMS] AS 

DROP TABLE IF EXISTS STA_ALL_ITEMS

DECLARE @max_clearing_date as date 

SET @max_clearing_date = (Select EOMONTH(MAX(CLEARING_DATE)) from TP3_ALL_ITEMS)

SELECT 
	CLIENT,
	COMPANY_CODE, 
	DOCUMENT_NUMBER, 
	LINE_ITEM,
	VENDOR_NUMBER,
	DOCUMENT_TYPE, 
	SPECIAL_GL_INDICATOR,
	PAYMENT_BLOCK,
	PAYMENT_TERMS, 
	SCB_INDICATOR,
	GL_ACCOUNT,
	CLEARING_DOCUMENT,
	CURRENCY,
	POSTING_DATE,
	CLEARING_DATE,
	AMOUNT_LOCAL,
	AMOUNT_DOCUMENT,
	REFERENCE,
	ITEM_TEXT,
	[YEAR],
	DAYS1,
	DAYS2,
	BASELINE_DATE,
	FILE_PATH,
	DEBIT_CREDIT,
	DOWNLOAD_DATE,
	CLEARING_DOCUMENT_YEAR,
	SRC_DOWNLOAD_DATE,
	VENDOR_NAME,
	ACCOUNTING_CLERK_NUMBER,
	ACCOUNTING_CLERK_NAME,
	ACCOUNTING_CLERK_USER,
	RECONCILIATION_ACCOUNT,
	VENDOR_NAME_CHINESE,
	VENDOR_COUNTRY,
	TRADING_PARTNER,
	COMPANY_NAME,
	CITY,
	COMPANY_NAME_SHORT,
	RUN_ID,
	RUN_DATE,
	DOCUMENT_POSTED_BY,
	REASON,
	REASON_DETAILS,
	CHINA_PUBLIC_HOLIDAY,
	DOMESTIC_3RD_PAYMENT, 
	OVERSEA_3RD_PAYMENT,
	OVERSEA_IC_PAYMENT,
	KEY_DATE,
	DUE_DATE,
	ARREARS_AFTER_NET,
	TRANSACTION_KEY,
	WHT,
	DUPLICATE
INTO dbo.STA_ALL_ITEMS 
FROM TP3_ALL_ITEMS

DELETE FROM STA_ALL_ITEMS WHERE 
CLEARING_DATE <= DATEADD(MONTH,-1,DATEADD(YEAR,-1,@max_clearing_date)) and CLEARING_DATE is not null 

EXEC o2c.P_EXECUTE_ETL_FUNCTION @Imp_Function = 'REMOVE_ZERO', @Imp_TableName = 'STA_ALL_ITEMS', @schema = 'dbo'
GO
/****** Object:  StoredProcedure [dbo].[P_STA_CREATE_SCHEMA_TABLES]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_STA_CREATE_SCHEMA_TABLES] AS 

DROP TABLE IF EXISTS DBO.STA_ALL_ITEMS_SCHEMA
SELECT TOP(500)* INTO DBO.STA_ALL_ITEMS_SCHEMA FROM DBO.STA_ALL_ITEMS

DROP TABLE IF EXISTS DBO.STA_IRB_FULL_SCHEMA
SELECT TOP(500)* INTO DBO.STA_IRB_FULL_SCHEMA FROM DBO.STA_IRB_FULL

DROP TABLE IF EXISTS DBO.STA_IRB_MONTHLY_SCHEMA
SELECT TOP(500)* INTO DBO.STA_IRB_MONTHLY_SCHEMA FROM DBO.STA_IRB_MONTHLY

DROP TABLE IF EXISTS DBO.STA_OPEN_ITEMS_MONTHLY_SCHEMA
SELECT TOP(500)* INTO DBO.STA_OPEN_ITEMS_MONTHLY_SCHEMA FROM DBO.STA_OPEN_ITEMS_MONTHLY


GO
/****** Object:  StoredProcedure [dbo].[P_STA_EXEC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_STA_EXEC] AS 

EXEC P_STA_IRB_FULL

EXEC P_STA_IRB_MONTHLY

EXEC P_STA_ALL_ITEMS

EXEC P_STA_OPEN_ITEMS_MONTHLY

EXEC P_STA_CREATE_SCHEMA_TABLES 
GO
/****** Object:  StoredProcedure [dbo].[P_STA_IRB_FULL]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_STA_IRB_FULL] AS 

-- Standard Model of all items --> Weekly Update --> Always create from Scratch
DROP TABLE IF EXISTS STA_IRB_FULL

DECLARE @max_posting_date as date 

SET @max_posting_date = (Select EOMONTH(MAX(CONVERT(date,POSTING_DATE,104))) from TP3_IRB)

SELECT 
            [ACTIVITY_STATUS]
           ,[YEAR]
           ,[CASH_DISCOUNT2]
           ,[CLEARING_DATE]
           ,[COMPANY_CODE]
           ,[CURRENCY]
           ,[ENTERED_ON_DATE]
           ,[FI_DOCUMENT_NO]
           ,[INPUT_DATE]
           ,[INVOICE_DATE]
           ,[INVOICE_INPUT_CHANNEL]
           ,[INVOICE_STATE]
           ,[LEGAL_ENTITY]
           ,[OCR_INVOICE_CORRECTION]
           ,[OCR_STACK_NAME]
           ,[OCR_SUPPLIER_CORRECTION]
           ,[PAYING_DATE]
           ,[POSTING_DATE]
           ,[PURCHASE_ORDER]
           ,[REFERENCE]
           ,[SCANTIME]
           ,[SOURCE_SYSTEM]
           ,[STATE_AUTO_POSTING]
           ,[VENDOR_NUMBER]
           ,[TEAM_HIST]
           ,[TRANSACTION_KEY]
           ,[AMOUNT_DOCUMENT]
           ,[AMOUNT_LOCAL]
           ,[TAX_AMOUNT]
           ,[AUTH_LEGAL_REGION]
           ,[ACTIVITY_STATUS_DESCRIPTION]
           ,[INVOICE_STATE_DESCRIPTION]
           ,[CURRENCY_DESCRIPTION]
           ,[SUPPLIER_CORRECTION_DESCRIPTION]
           ,[STATE_AUTO_POSTING_DESCRIPTION]
           ,[INVOICE_CORRECTION_DESCRIPTION]
           ,[SCAN_DATE]
           ,[DELIVERY_NOTE]
           ,[MAT_DOCUMENT_NO]
           ,[SRC_DOWNLOAD_DATE]
           ,[TS_FILENAME]
           ,[TS_FAPIAO_CODE]
           ,[PURCHASING_GROUP]
           ,[PO_CREATED_BY]
           ,[PO_COMPANY_CODE]
           ,[REQUISITIONER]
           ,[PR_CREATOR]
           ,[COMPANY_NAME]
           ,[COMPANY_NAME_SHORT]
           ,[PURCHASER_NAME]
           ,[GR_QUANTITY]
           ,[IR_QUANTITY]
           ,[TS_ERROR_01]
           ,[TS_ERROR_02]
           ,[TS_ERROR_03]
           ,[TS_ERROR_04]
           ,[TS_ERROR_05]
           ,[TS_ERROR_06]
           ,[TS_ERROR_07]
           ,[TS_ERROR_08]
           ,[TS_PO_REMARK]
           ,[ACCOUNTING_CLERK_NAME]
           ,[ACCOUNTING_CLERK_NUMBER]
           ,[ACCOUNTING_CLERK_USER]
           ,[RECONCILIATION_ACCOUNT]
           ,[TRADING_PARTNER]
           ,[VENDOR_COUNTRY]
           ,[VENDOR_NAME]
           ,[VENDOR_NAME_CHINESE]
           ,[REFERENCE_DOCUMENT]
           ,[YEAR_REF_DOC]
           ,[REF_DOC_ENTRY_DATE]
           ,[REF_DOC_POSTING_DATE]
           ,[REF_DOC_CREATED_BY]
           ,[SCAN_DATE_TO_INPUT_DATE]
           ,[INPUT_DATE_TO_POSTING_DATE]
           ,[EIV_AUTOPOST]
           ,[KEY_DATE]
     INTO dbo.STA_IRB_FULL 
	 FROM TP3_IRB

DELETE FROM STA_IRB_FULL WHERE 
CONVERT(date,POSTING_DATE,104) <= DATEADD(MONTH,-2,DATEADD(YEAR,-1,@max_posting_date))

EXEC o2c.P_EXECUTE_ETL_FUNCTION @Imp_Function = 'REMOVE_ZERO', @Imp_TableName = 'STA_IRB_FULL', @schema = 'dbo'
GO
/****** Object:  StoredProcedure [dbo].[P_STA_IRB_MONTHLY]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_STA_IRB_MONTHLY] AS 

-- MONTHLY: Insert all lines into the table that don't have the same month

DECLARE @Keydate_TP3 date 
DECLARE @Keydate_STA date

SET @Keydate_TP3 = (SELECT MAX(KEY_DATE) FROM TP3_IRB)
SET @Keydate_STA = (SELECT MAX(KEY_DATE) FROM STA_IRB_MONTHLY)

IF @Keydate_TP3 > @Keydate_STA 
BEGIN

INSERT INTO [dbo].[STA_IRB_MONTHLY]
           ([ACTIVITY_STATUS]
           ,[YEAR]
           ,[CASH_DISCOUNT2]
           ,[CLEARING_DATE]
           ,[COMPANY_CODE]
           ,[CURRENCY]
           ,[ENTERED_ON_DATE]
           ,[FI_DOCUMENT_NO]
           ,[INPUT_DATE]
           ,[INVOICE_DATE]
           ,[INVOICE_INPUT_CHANNEL]
           ,[INVOICE_STATE]
           ,[LEGAL_ENTITY]
           ,[OCR_INVOICE_CORRECTION]
           ,[OCR_STACK_NAME]
           ,[OCR_SUPPLIER_CORRECTION]
           ,[PAYING_DATE]
           ,[POSTING_DATE]
           ,[PURCHASE_ORDER]
           ,[REFERENCE]
           ,[SCANTIME]
           ,[SOURCE_SYSTEM]
           ,[STATE_AUTO_POSTING]
           ,[VENDOR_NUMBER]
           ,[TEAM_HIST]
           ,[TRANSACTION_KEY]
           ,[AMOUNT_DOCUMENT]
           ,[AMOUNT_LOCAL]
           ,[TAX_AMOUNT]
           ,[AUTH_LEGAL_REGION]
           ,[ACTIVITY_STATUS_DESCRIPTION]
           ,[INVOICE_STATE_DESCRIPTION]
           ,[CURRENCY_DESCRIPTION]
           ,[SUPPLIER_CORRECTION_DESCRIPTION]
           ,[STATE_AUTO_POSTING_DESCRIPTION]
           ,[INVOICE_CORRECTION_DESCRIPTION]
           ,[SCAN_DATE]
           ,[DELIVERY_NOTE]
           ,[MAT_DOCUMENT_NO]
           ,[SRC_DOWNLOAD_DATE]
           ,[TS_FILENAME]
           ,[TS_FAPIAO_CODE]
           ,[PURCHASING_GROUP]
           ,[PO_CREATED_BY]
           ,[PO_COMPANY_CODE]
           ,[REQUISITIONER]
           ,[PR_CREATOR]
           ,[COMPANY_NAME]
           ,[COMPANY_NAME_SHORT]
           ,[PURCHASER_NAME]
           ,[GR_QUANTITY]
           ,[IR_QUANTITY]
           ,[TS_ERROR_01]
           ,[TS_ERROR_02]
           ,[TS_ERROR_03]
           ,[TS_ERROR_04]
           ,[TS_ERROR_05]
           ,[TS_ERROR_06]
           ,[TS_ERROR_07]
           ,[TS_ERROR_08]
           ,[TS_PO_REMARK]
           ,[ACCOUNTING_CLERK_NAME]
           ,[ACCOUNTING_CLERK_NUMBER]
           ,[ACCOUNTING_CLERK_USER]
           ,[RECONCILIATION_ACCOUNT]
           ,[TRADING_PARTNER]
           ,[VENDOR_COUNTRY]
           ,[VENDOR_NAME]
           ,[VENDOR_NAME_CHINESE]
           ,[REFERENCE_DOCUMENT]
           ,[YEAR_REF_DOC]
           ,[REF_DOC_ENTRY_DATE]
           ,[REF_DOC_POSTING_DATE]
           ,[REF_DOC_CREATED_BY]
           ,[SCAN_DATE_TO_INPUT_DATE]
           ,[INPUT_DATE_TO_POSTING_DATE]
           ,[EIV_AUTOPOST]
           ,[KEY_DATE]
		   ,[AMOUNT_EUR])
SELECT [ACTIVITY_STATUS]
           ,[YEAR]
           ,[CASH_DISCOUNT2]
           ,[CLEARING_DATE]
           ,[COMPANY_CODE]
           ,[CURRENCY]
           ,[ENTERED_ON_DATE]
           ,[FI_DOCUMENT_NO]
           ,[INPUT_DATE]
           ,[INVOICE_DATE]
           ,[INVOICE_INPUT_CHANNEL]
           ,[INVOICE_STATE]
           ,[LEGAL_ENTITY]
           ,[OCR_INVOICE_CORRECTION]
           ,[OCR_STACK_NAME]
           ,[OCR_SUPPLIER_CORRECTION]
           ,[PAYING_DATE]
           ,[POSTING_DATE]
           ,[PURCHASE_ORDER]
           ,[REFERENCE]
           ,[SCANTIME]
           ,[SOURCE_SYSTEM]
           ,[STATE_AUTO_POSTING]
           ,[VENDOR_NUMBER]
           ,[TEAM_HIST]
           ,[TRANSACTION_KEY]
           ,[AMOUNT_DOCUMENT]
           ,[AMOUNT_LOCAL]
           ,[TAX_AMOUNT]
           ,[AUTH_LEGAL_REGION]
           ,[ACTIVITY_STATUS_DESCRIPTION]
           ,[INVOICE_STATE_DESCRIPTION]
           ,[CURRENCY_DESCRIPTION]
           ,[SUPPLIER_CORRECTION_DESCRIPTION]
           ,[STATE_AUTO_POSTING_DESCRIPTION]
           ,[INVOICE_CORRECTION_DESCRIPTION]
           ,[SCAN_DATE]
           ,[DELIVERY_NOTE]
           ,[MAT_DOCUMENT_NO]
           ,[SRC_DOWNLOAD_DATE]
           ,[TS_FILENAME]
           ,[TS_FAPIAO_CODE]
           ,[PURCHASING_GROUP]
           ,[PO_CREATED_BY]
           ,[PO_COMPANY_CODE]
           ,[REQUISITIONER]
           ,[PR_CREATOR]
           ,[COMPANY_NAME]
           ,[COMPANY_NAME_SHORT]
           ,[PURCHASER_NAME]
           ,[GR_QUANTITY]
           ,[IR_QUANTITY]
           ,[TS_ERROR_01]
           ,[TS_ERROR_02]
           ,[TS_ERROR_03]
           ,[TS_ERROR_04]
           ,[TS_ERROR_05]
           ,[TS_ERROR_06]
           ,[TS_ERROR_07]
           ,[TS_ERROR_08]
           ,[TS_PO_REMARK]
           ,[ACCOUNTING_CLERK_NAME]
           ,[ACCOUNTING_CLERK_NUMBER]
           ,[ACCOUNTING_CLERK_USER]
           ,[RECONCILIATION_ACCOUNT]
           ,[TRADING_PARTNER]
           ,[VENDOR_COUNTRY]
           ,[VENDOR_NAME]
           ,[VENDOR_NAME_CHINESE]
           ,[REFERENCE_DOCUMENT]
           ,[YEAR_REF_DOC]
           ,[REF_DOC_ENTRY_DATE]
           ,[REF_DOC_POSTING_DATE]
           ,[REF_DOC_CREATED_BY]
           ,[SCAN_DATE_TO_INPUT_DATE]
           ,[INPUT_DATE_TO_POSTING_DATE]
           ,[EIV_AUTOPOST]
           ,[KEY_DATE]
		   ,[AMOUNT_EUR]
FROM TP3_IRB
WHERE INVOICE_STATE = '11' OR INVOICE_STATE = '12'


-- Update Slowly changing Dimensions VENDOR and Company and EKKO

UPDATE
    IRB
SET 
	IRB.ACCOUNTING_CLERK_NAME = VENDOR.ACCOUNTING_CLERK_NAME,
	IRB.ACCOUNTING_CLERK_NUMBER = VENDOR.ACCOUNTING_CLERK_NUMBER, 
	IRB.ACCOUNTING_CLERK_USER = VENDOR.ACCOUNTING_CLERK_USER, 
	IRB.VENDOR_NAME = VENDOR.VENDOR_NAME, 
	IRB.VENDOR_NAME_CHINESE = VENDOR.VENDOR_NAME_CHINESE, 
	IRB.VENDOR_COUNTRY = VENDOR.VENDOR_COUNTRY,
	IRB.COMPANY_NAME = COMPANY.COMPANY_NAME,
	IRB.COMPANY_NAME_SHORT = COMPANY.COMPANY_NAME_SHORT, 
	IRB.PURCHASER_NAME = PURCHASER.PURCHASER_NAME
FROM
    STA_IRB_MONTHLY AS IRB
	LEFT JOIN
	TP1_VENDOR_DIMENSION AS VENDOR ON
    CAST(IRB.VENDOR_NUMBER as int) = CAST(VENDOR.VENDOR_NUMBER as int) AND 
	IRB.COMPANY_CODE = VENDOR.COMPANY_CODE
	LEFT JOIN 
	CLN_T001 AS COMPANY ON 
	IRB.COMPANY_CODE = COMPANY.COMPANY_CODE
	LEFT JOIN 
	CLN_T024 AS PURCHASER ON 
	IRB.PURCHASING_GROUP = PURCHASER.PURCHASING_GROUP

EXEC o2c.P_EXECUTE_ETL_FUNCTION @Imp_Function = 'REMOVE_ZERO', @Imp_TableName = 'STA_IRB_MONTHLY', @schema = 'dbo'

END
GO
/****** Object:  StoredProcedure [dbo].[P_STA_OPEN_ITEMS_MONTHLY]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[P_STA_OPEN_ITEMS_MONTHLY] AS

DECLARE @KEYDATE_TP3 DATE
DECLARE @MONTH INT 
SET @MONTH = 0 

SET @KEYDATE_TP3 = (SELECT MAX(KEY_DATE) FROM DBO.TP3_ALL_ITEMS)

DROP TABLE IF EXISTS STA_OPEN_ITEMS_MONTHLY

SELECT 
CLIENT,
	COMPANY_CODE, 
	DOCUMENT_NUMBER, 
	LINE_ITEM,
	VENDOR_NUMBER,
	DOCUMENT_TYPE, 
	SPECIAL_GL_INDICATOR,
	PAYMENT_BLOCK,
	PAYMENT_TERMS, 
	SCB_INDICATOR,
	GL_ACCOUNT,
	CLEARING_DOCUMENT,
	CURRENCY,
	POSTING_DATE,
	CLEARING_DATE,
	AMOUNT_LOCAL,
	AMOUNT_DOCUMENT,
	REFERENCE,
	ITEM_TEXT,
	[YEAR],
	DAYS1,
	DAYS2,
	BASELINE_DATE,
	FILE_PATH,
	DEBIT_CREDIT,
	DOWNLOAD_DATE,
	CLEARING_DOCUMENT_YEAR,
	SRC_DOWNLOAD_DATE,
	VENDOR_NAME,
	ACCOUNTING_CLERK_NUMBER,
	ACCOUNTING_CLERK_NAME,
	ACCOUNTING_CLERK_USER,
	RECONCILIATION_ACCOUNT,
	VENDOR_NAME_CHINESE,
	VENDOR_COUNTRY,
	TRADING_PARTNER,
	COMPANY_NAME,
	CITY,
	COMPANY_NAME_SHORT,
	RUN_ID,
	RUN_DATE,
	DOCUMENT_POSTED_BY,
	REASON,
	REASON_DETAILS,
	CHINA_PUBLIC_HOLIDAY,
	DOMESTIC_3RD_PAYMENT, 
	OVERSEA_3RD_PAYMENT,
	OVERSEA_IC_PAYMENT,
	KEY_DATE,
	DUE_DATE,
	ARREARS_AFTER_NET,
	TRANSACTION_KEY,
	WHT,
	DUPLICATE
INTO DBO.STA_OPEN_ITEMS_MONTHLY
FROM DBO.TP3_ALL_ITEMS
WHERE POSTING_DATE <= @KEYDATE_TP3 AND 
	( CLEARING_DATE IS NULL OR CLEARING_DATE > @KEYDATE_TP3 );


WHILE (@MONTH > -11)
BEGIN 

SET @KEYDATE_TP3 = EOMONTH(DATEADD(MONTH,-1,@KEYDATE_TP3))

INSERT INTO DBO.STA_OPEN_ITEMS_MONTHLY (
CLIENT,
	COMPANY_CODE, 
	DOCUMENT_NUMBER, 
	LINE_ITEM,
	VENDOR_NUMBER,
	DOCUMENT_TYPE, 
	SPECIAL_GL_INDICATOR,
	PAYMENT_BLOCK,
	PAYMENT_TERMS, 
	SCB_INDICATOR,
	GL_ACCOUNT,
	CLEARING_DOCUMENT,
	CURRENCY,
	POSTING_DATE,
	CLEARING_DATE,
	AMOUNT_LOCAL,
	AMOUNT_DOCUMENT,
	REFERENCE,
	ITEM_TEXT,
	[YEAR],
	DAYS1,
	DAYS2,
	BASELINE_DATE,
	FILE_PATH,
	DEBIT_CREDIT,
	DOWNLOAD_DATE,
	CLEARING_DOCUMENT_YEAR,
	SRC_DOWNLOAD_DATE,
	VENDOR_NAME,
	ACCOUNTING_CLERK_NUMBER,
	ACCOUNTING_CLERK_NAME,
	ACCOUNTING_CLERK_USER,
	RECONCILIATION_ACCOUNT,
	VENDOR_NAME_CHINESE,
	VENDOR_COUNTRY,
	TRADING_PARTNER,
	COMPANY_NAME,
	CITY,
	COMPANY_NAME_SHORT,
	RUN_ID,
	RUN_DATE,
	DOCUMENT_POSTED_BY,
	REASON,
	REASON_DETAILS,
	CHINA_PUBLIC_HOLIDAY,
	DOMESTIC_3RD_PAYMENT, 
	OVERSEA_3RD_PAYMENT,
	OVERSEA_IC_PAYMENT,
	KEY_DATE,
	DUE_DATE,
	ARREARS_AFTER_NET,
	TRANSACTION_KEY,
	WHT,
	DUPLICATE)
SELECT 
CLIENT,
	COMPANY_CODE, 
	DOCUMENT_NUMBER, 
	LINE_ITEM,
	VENDOR_NUMBER,
	DOCUMENT_TYPE, 
	SPECIAL_GL_INDICATOR,
	PAYMENT_BLOCK,
	PAYMENT_TERMS, 
	SCB_INDICATOR,
	GL_ACCOUNT,
	CLEARING_DOCUMENT,
	CURRENCY,
	POSTING_DATE,
	CLEARING_DATE,
	AMOUNT_LOCAL,
	AMOUNT_DOCUMENT,
	REFERENCE,
	ITEM_TEXT,
	[YEAR],
	DAYS1,
	DAYS2,
	BASELINE_DATE,
	FILE_PATH,
	DEBIT_CREDIT,
	DOWNLOAD_DATE,
	CLEARING_DOCUMENT_YEAR,
	SRC_DOWNLOAD_DATE,
	VENDOR_NAME,
	ACCOUNTING_CLERK_NUMBER,
	ACCOUNTING_CLERK_NAME,
	ACCOUNTING_CLERK_USER,
	RECONCILIATION_ACCOUNT,
	VENDOR_NAME_CHINESE,
	VENDOR_COUNTRY,
	TRADING_PARTNER,
	COMPANY_NAME,
	CITY,
	COMPANY_NAME_SHORT,
	RUN_ID,
	RUN_DATE,
	DOCUMENT_POSTED_BY,
	REASON,
	REASON_DETAILS,
	CHINA_PUBLIC_HOLIDAY,
	DOMESTIC_3RD_PAYMENT, 
	OVERSEA_3RD_PAYMENT,
	OVERSEA_IC_PAYMENT,
	NULL,
	DUE_DATE,
	ARREARS_AFTER_NET,
	TRANSACTION_KEY,
	WHT,
	DUPLICATE
FROM DBO.TP3_ALL_ITEMS
WHERE POSTING_DATE <= @KEYDATE_TP3 AND 
	( CLEARING_DATE IS NULL OR CLEARING_DATE > @KEYDATE_TP3 );

UPDATE DBO.STA_OPEN_ITEMS_MONTHLY SET KEY_DATE = @KEYDATE_TP3 
WHERE KEY_DATE IS NULL

SET @MONTH = @MONTH -1 
END

UPDATE DBO.STA_OPEN_ITEMS_MONTHLY SET ARREARS_AFTER_NET = DBO.FC_CALCULATE_ARREARS(DUE_DATE,KEY_DATE) 

EXEC o2c.P_EXECUTE_ETL_FUNCTION @IMP_FUNCTION = 'REMOVE_ZERO', @IMP_TABLENAME = 'STA_OPEN_ITEMS_MONTHLY', @schema = 'dbo'
GO
/****** Object:  StoredProcedure [dbo].[P_TP1_EXEC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP1_EXEC] AS

--TP1_EKBE-------------------------------------------------------------------------------------------

EXEC P_TP10_EKBE

--TP1_VENDOR_DIMENSION---------------------------------------------------------------------------

EXEC P_TP10_VENDOR_DIM

-- TP1_IRB Table --------------------------------------------------------------------------------

EXEC P_TP10_IRB
GO
/****** Object:  StoredProcedure [dbo].[P_TP10_EKBE]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP10_EKBE] AS

DROP TABLE IF EXISTS #EKBE_Q
DROP TABLE IF EXISTS #EKBE_E
DROP TABLE IF EXISTS TP1_EKBE;

CREATE TABLE #EKBE_Q(
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]   

CREATE NONCLUSTERED INDEX ekbe_q_ref
    ON #ekbe_q (reference_document, year_ref_doc);   

CREATE TABLE #EKBE_E(
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
-- for reverse movement types reverse quantity 

CREATE NONCLUSTERED INDEX ekbe_e_ref
    ON #ekbe_e (reference_document, year_ref_doc,ENTRY_DATE DESC, MATERIAL_DOCUMENT DESC );   

UPDATE CLN_EKBE SET QUANTITY = QUANTITY * -1 WHERE MOVEMENT_TYPE = '102' OR MOVEMENT_TYPE = '122';

WITH EKBE_SUM_Q AS (
    SELECT 
        *,
		SUM(QUANTITY) OVER (
            PARTITION BY 
                PURCHASE_ORDER
        ) QUANTITY_SUM,
        ROW_NUMBER() OVER (
            PARTITION BY 
			PURCHASE_ORDER 
            ORDER BY 
                PURCHASE_ORDER
        ) ROW_NUM
     FROM 
        CLN_EKBE
	WHERE PO_HISTORY_CATEGORY = 'Q'
	AND REFERENCE_DOCUMENT <> ''
)

INSERT INTO #EKBE_Q ( 
	 [PURCHASE_ORDER]
	,[MATERIAL_DOCUMENT] 
	,[YEAR_MAT_DOC] 
	,[REFERENCE_DOCUMENT]
	,[YEAR_REF_DOC] 
	,[POSTING_DATE]  
	,[ENTRY_DATE] 
	,[CREATED_BY] 
	,[PO_HISTORY_CATEGORY] 
	,[PLANT] 
	,[QUANTITY] 
	,[FILE_PATH]
	,[AA_NUMBER]
	,[MOVEMENT_TYPE] 
	,[DOWNLOAD_DATE] 
)
SELECT 
	[PURCHASE_ORDER]
	,[MATERIAL_DOCUMENT] 
	,[YEAR_MAT_DOC] 
	,[REFERENCE_DOCUMENT]
	,[YEAR_REF_DOC] 
	,[POSTING_DATE]  
	,[ENTRY_DATE] 
	,[CREATED_BY] 
	,[PO_HISTORY_CATEGORY] 
	,[PLANT] 
	,[QUANTITY_SUM] 
	,[FILE_PATH]
	,[AA_NUMBER]
	,[MOVEMENT_TYPE] 
	,[DOWNLOAD_DATE] 
	FROM EKBE_SUM_Q;

WITH EKBE_SUM_E AS (
    SELECT 
        *,
		SUM(QUANTITY) OVER (
            PARTITION BY 
                PURCHASE_ORDER
        ) QUANTITY_SUM,
        ROW_NUMBER() OVER (
            PARTITION BY 
			PURCHASE_ORDER 
            ORDER BY 
                PURCHASE_ORDER
        ) ROW_NUM
     FROM 
        CLN_EKBE
	WHERE PO_HISTORY_CATEGORY = 'E'
		AND REFERENCE_DOCUMENT <> ''
)


INSERT INTO #EKBE_E ( 
	 [PURCHASE_ORDER]
	,[MATERIAL_DOCUMENT] 
	,[YEAR_MAT_DOC] 
	,[REFERENCE_DOCUMENT]
	,[YEAR_REF_DOC] 
	,[POSTING_DATE]  
	,[ENTRY_DATE] 
	,[CREATED_BY] 
	,[PO_HISTORY_CATEGORY] 
	,[PLANT] 
	,[QUANTITY] 
	,[FILE_PATH]
	,[AA_NUMBER]
	,[MOVEMENT_TYPE] 
	,[DOWNLOAD_DATE] 
)
SELECT 
	[PURCHASE_ORDER]
	,[MATERIAL_DOCUMENT] 
	,[YEAR_MAT_DOC] 
	,[REFERENCE_DOCUMENT]
	,[YEAR_REF_DOC] 
	,[POSTING_DATE]  
	,[ENTRY_DATE] 
	,[CREATED_BY] 
	,[PO_HISTORY_CATEGORY] 
	,[PLANT] 
	,[QUANTITY_SUM] 
	,[FILE_PATH]
	,[AA_NUMBER]
	,[MOVEMENT_TYPE] 
	,[DOWNLOAD_DATE] 
	FROM EKBE_SUM_E;

WITH EKBE_Delete_suplicates AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
			REFERENCE_DOCUMENT,
			YEAR_REF_DOC
            ORDER BY 
            REFERENCE_DOCUMENT, 
			YEAR_REF_DOC,
			ENTRY_DATE DESC,
			MATERIAL_DOCUMENT DESC
        ) ROW_NUM
     FROM 
        #EKBE_E
	WHERE PO_HISTORY_CATEGORY = 'E'
)

DELETE FROM EKBE_Delete_suplicates
where ROW_NUM > 1

SELECT Q.MATERIAL_DOCUMENT, 
	   Q.YEAR_MAT_DOC, 
	   E.MATERIAL_DOCUMENT as REFERENCE_DOCUMENT,
	   E.YEAR_REF_DOC as YEAR_REF_DOC,
	   E.ENTRY_DATE as REF_DOC_ENTRY_DATE,
	   E.POSTING_DATE as REF_DOC_POSTING_DATE,
	   E.CREATED_BY as REF_DOC_CREATED_BY,
	   E.QUANTITY as GR_QUANTITY,
	   Q.QUANTITY as IR_QUANTITY
INTO DBO.TP1_EKBE FROM #EKBE_Q AS Q
LEFT OUTER JOIN #EKBE_E AS E ON 
Q.REFERENCE_DOCUMENT = E.REFERENCE_DOCUMENT AND 
Q.YEAR_REF_DOC = E.YEAR_REF_DOC;

WITH EKBE_DEL_DUPLICATES AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                YEAR_MAT_DOC, 
                MATERIAL_DOCUMENT
            ORDER BY 
                YEAR_MAT_DOC,
				MATERIAL_DOCUMENT, 
				REF_DOC_ENTRY_DATE DESC
        ) ROW_NUM
     FROM 
     TP1_EKBE
)

DELETE FROM EKBE_DEL_DUPLICATES
WHERE ROW_NUM > 1

DROP TABLE IF EXISTS #EKBE_Q
DROP TABLE IF EXISTS #EKBE_E
GO
/****** Object:  StoredProcedure [dbo].[P_TP10_IRB]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP10_IRB] AS 

DROP TABLE IF EXISTS dbo.TP1_IRB

SELECT 
	 IRB.*,
	 OCRLOG.TS_FILENAME, 
	 OCRLOG.TS_FAPIAO_CODE, 
	 EKKO.PURCHASING_GROUP, 
	 EKKO.PO_CREATED_BY, 
	 EKKO.COMPANY_CODE AS PO_COMPANY_CODE,
	 EBAN.REQUISITIONER, 
	 EBAN.PR_CREATOR,
	 COMPANY.COMPANY_NAME, 
	 COMPANY.COMPANY_NAME_SHORT,
	 PURCHASER.PURCHASER_NAME
INTO dbo.TP1_IRB
FROM CLN_FI5000 AS IRB 
	 LEFT JOIN 
	 CLN_OCRLOG AS OCRLOG ON 
	 IRB.OCR_STACK_NAME = OCRLOG.TIF_FILE 
	 LEFT JOIN 
	 CLN_EKKO AS EKKO ON 
	 IRB.PURCHASE_ORDER = EKKO.PURCHASE_ORDER 
	 LEFT JOIN 
	 CLN_EBAN AS EBAN ON 
	 IRB.PURCHASE_ORDER = EBAN.PURCHASE_ORDER
	 LEFT JOIN 
	 CLN_T024 AS PURCHASER ON 
	 EKKO.PURCHASING_GROUP = PURCHASER.PURCHASING_GROUP
	 LEFT JOIN
	 CLN_T001 AS COMPANY ON 
	 IRB.COMPANY_CODE = COMPANY.COMPANY_CODE
GO
/****** Object:  StoredProcedure [dbo].[P_TP10_VENDOR_DIM]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP10_VENDOR_DIM] AS

DROP TABLE IF EXISTS TP1_VENDOR_DIMENSION

SELECT  
		VENDOR.CLIENT, 
		VENDOR.COMPANY_CODE, 
		VENDOR.VENDOR_NUMBER, 
		CONCAT(VENDOR.VENDOR_NAME1,' ', VENDOR.VENDOR_NAME2) AS VENDOR_NAME,
		VENDOR.ACCOUNTING_CLERK_NUMBER, 
		VENDOR.RECONCILIATION_ACCOUNT, 
		VENDOR.VENDOR_COUNTRY, 
		VENDOR.TRADING_PARTNER,
		CONCAT(NAMECN.NAME1, ' ', NAMECN.NAME2) AS VENDOR_NAME_CHINESE, 
		CLERK.ACCOUNTING_CLERK_NAME, 
		CLERK.ACCOUNTING_CLERK_USER
INTO DBO.TP1_VENDOR_DIMENSION 
FROM CLN_VF_KRED AS VENDOR 
	 LEFT OUTER JOIN 
	 CLN_T001S AS CLERK ON 
	 VENDOR.ACCOUNTING_CLERK_NUMBER = CLERK.ACCOUNTING_CLERK_NUMBER AND 
	 VENDOR.COMPANY_CODE = CLERK.COMPANY_CODE 
	 LEFT OUTER JOIN
	 CLN_ADRC AS NAMECN ON 
	 NAMECN.ADDRESS_NUMBER = VENDOR.ADDRESS_NUMBER

UPDATE TP1_VENDOR_DIMENSION SET VENDOR_NAME_CHINESE = VENDOR_NAME
WHERE VENDOR_NAME_CHINESE = ''
GO
/****** Object:  StoredProcedure [dbo].[P_TP2_EXEC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP2_EXEC] AS 

BEGIN TRANSACTION

--TP2_OPEN_ITEMS--------------------------------------------------------------------------------

EXEC P_TP20_OPEN_ITEMS

--TP2_CLEARED_ITEMS------------------------------------------------------------------------------

EXEC P_TP20_CLEARED_ITEMS


-- TP2_IRB-------------------------------------------------------------------------------
EXEC P_TP20_IRB

COMMIT TRANSACTION
GO
/****** Object:  StoredProcedure [dbo].[P_TP20_CLEARED_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP20_CLEARED_ITEMS] AS 

DROP TABLE IF EXISTS DBO.TP2_CLEARED_ITEMS 

SELECT  
	CI.*,
	VENDOR.VENDOR_NAME, 
	VENDOR.ACCOUNTING_CLERK_NUMBER, 
	VENDOR.ACCOUNTING_CLERK_NAME,
	VENDOR.ACCOUNTING_CLERK_USER, 
	VENDOR.RECONCILIATION_ACCOUNT,
	VENDOR.VENDOR_NAME_CHINESE,
	VENDOR.VENDOR_COUNTRY,
	VENDOR.TRADING_PARTNER,
	COMPANY.COMPANY_NAME,
	COMPANY.CITY, 
	COMPANY.COMPANY_NAME_SHORT,
	RUNS.RUN_ID,
	RUNS.RUN_DATE, 
	BKPF.DOCUMENT_POSTED_BY, 
	OVERDUE.REASON, 
	OVERDUE.REASON_DETAILS,
	CAL.CHINA_PUBLIC_HOLIDAY,
	CAL.DOMESTIC_3RD_PAYMENT, 
	CAL.OVERSEA_3RD_PAYMENT, 
	CAL.OVERSEA_IC_PAYMENT
INTO DBO.TP2_CLEARED_ITEMS
FROM CLN_BSAK AS CI 
	 LEFT JOIN 
	 TP1_VENDOR_DIMENSION AS VENDOR ON 
	 CI.VENDOR_NUMBER = VENDOR.VENDOR_NUMBER AND 
	 CI.COMPANY_CODE = VENDOR.COMPANY_CODE 
	 LEFT JOIN 
	 CLN_T001 AS COMPANY ON 
	 CI.COMPANY_CODE = COMPANY.COMPANY_CODE 
	 LEFT JOIN 
	 CLN_REGUP AS RUNS ON 
	 CI.CLEARING_DOCUMENT = RUNS.PAYMENT_DOCUMENT AND 
	 CI.COMPANY_CODE = RUNS.COMPANY_CODE
	 LEFT JOIN 
	 CLN_BKPF AS BKPF ON 
	 CI.DOCUMENT_NUMBER = BKPF.DOCUMENT_NUMBER AND
	 CI.[YEAR] =  BKPF.FISCAL_YEAR AND 
	 CI.COMPANY_CODE = BKPF.COMPANY_CODE
	 LEFT JOIN 
	 CLN_OVERDUE_REASON AS OVERDUE ON 
	 CI.DOCUMENT_NUMBER = OVERDUE.DOCUMENT_NUMBER AND 
	 CI.[YEAR] = OVERDUE.[YEAR] AND 
	 CI.LINE_ITEM = OVERDUE.LINE_ITEM 
	 LEFT JOIN 
	 CLN_PAYMENT_CALENDAR AS CAL ON
	 CI.CLEARING_DATE = CAST(CAL.DATES AS DATETIME)
GO
/****** Object:  StoredProcedure [dbo].[P_TP20_IRB]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP20_IRB] AS 

DROP TABLE IF EXISTS TP2_IRB

SELECT 
	 IRB1.*,
	 TS.TS_ERROR_01, 
	 TS.TS_ERROR_02, 
	 TS.TS_ERROR_03, 
	 TS.TS_ERROR_04, 
	 TS.TS_ERROR_05, 
	 TS.TS_ERROR_06, 
	 TS.TS_ERROR_07, 
	 TS.TS_ERROR_08, 
	 TS.TS_PO_REMARK, 
	 VENDOR.ACCOUNTING_CLERK_NAME, 
	 VENDOR.ACCOUNTING_CLERK_NUMBER, 
	 VENDOR.ACCOUNTING_CLERK_USER, 
	 VENDOR.RECONCILIATION_ACCOUNT,
	 VENDOR.TRADING_PARTNER, 
	 VENDOR.VENDOR_COUNTRY, 
	 VENDOR.VENDOR_NAME, 
	 VENDOR.VENDOR_NAME_CHINESE, 
	 GRDATE.REFERENCE_DOCUMENT, 
	 GRDATE.YEAR_REF_DOC, 
	 GRDATE.REF_DOC_ENTRY_DATE, 
	 GRDATE.REF_DOC_POSTING_DATE, 
	 GRDATE.REF_DOC_CREATED_BY,
	 GRDATE.GR_QUANTITY, 
	 GRDATE.IR_QUANTITY
INTO dbo.TP2_IRB
FROM TP1_IRB AS IRB1
	 LEFT JOIN CLN_TRADESHIFT_INVOICES AS TS ON 
	 IRB1.TS_FAPIAO_CODE = TS.TS_FAPIAO_CODE AND 
	 IRB1.REFERENCE = TS.TS_REFERENCE 
	 LEFT JOIN TP1_VENDOR_DIMENSION AS VENDOR ON 
	 IRB1.VENDOR_NUMBER  = VENDOR.VENDOR_NUMBER AND 
	 IRB1.COMPANY_CODE = VENDOR.COMPANY_CODE 
	 LEFT JOIN TP1_EKBE AS GRDATE ON 
	 IRB1.[YEAR] = GRDATE.YEAR_MAT_DOC AND
	 IRB1.MAT_DOCUMENT_NO = GRDATE.MATERIAL_DOCUMENT


GO
/****** Object:  StoredProcedure [dbo].[P_TP20_OPEN_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE  [dbo].[P_TP20_OPEN_ITEMS] AS

DROP TABLE IF EXISTS dbo.TP2_OPEN_ITEMS 

SELECT  
	OI.*,
	VENDOR.VENDOR_NAME, 
	VENDOR.ACCOUNTING_CLERK_NUMBER, 
	VENDOR.ACCOUNTING_CLERK_NAME,
	VENDOR.ACCOUNTING_CLERK_USER, 
	VENDOR.RECONCILIATION_ACCOUNT,
	VENDOR.VENDOR_NAME_CHINESE,
	VENDOR.VENDOR_COUNTRY,
	VENDOR.TRADING_PARTNER,
	COMPANY.COMPANY_NAME,
	COMPANY.CITY, 
	COMPANY.COMPANY_NAME_SHORT,
	RUNS.RUN_ID,
	RUNS.RUN_DATE,
	BKPF.DOCUMENT_POSTED_BY, 
    OVERDUE.REASON, 
	OVERDUE.REASON_DETAILS,
	CAL.CHINA_PUBLIC_HOLIDAY,
	CAL.DOMESTIC_3RD_PAYMENT, 
	CAL.OVERSEA_3RD_PAYMENT, 
	CAL.OVERSEA_IC_PAYMENT
INTO dbo.TP2_OPEN_ITEMS 
FROM CLN_BSIK AS OI 
	 LEFT JOIN 
	 TP1_VENDOR_DIMENSION AS VENDOR ON 
	 OI.VENDOR_NUMBER = VENDOR.VENDOR_NUMBER AND 
	 OI.COMPANY_CODE = VENDOR.COMPANY_CODE 
	 LEFT JOIN
	 CLN_T001 AS COMPANY ON OI.COMPANY_CODE = COMPANY.COMPANY_CODE
	 LEFT JOIN
	 CLN_REGUP AS RUNS ON 
	 OI.CLEARING_DOCUMENT = RUNS.PAYMENT_DOCUMENT AND 
	 OI.CLEARING_DOCUMENT_YEAR = RUNS.PAYMENT_DOCUMENT_YEAR AND 
	 OI.COMPANY_CODE = RUNS.COMPANY_CODE
	 LEFT JOIN 
	 CLN_BKPF AS BKPF ON 
	 OI.DOCUMENT_NUMBER = BKPF.DOCUMENT_NUMBER AND
	 OI.[YEAR] =  BKPF.FISCAL_YEAR AND 
	 OI.COMPANY_CODE = BKPF.COMPANY_CODE 
	 LEFT JOIN 
	 CLN_OVERDUE_REASON AS OVERDUE ON 
	 OI.DOCUMENT_NUMBER = OVERDUE.DOCUMENT_NUMBER AND
	 OI.COMPANY_CODE = OVERDUE.COMPANY_CODE AND
	 OI.[YEAR] = OVERDUE.[YEAR] AND 
	 OI.LINE_ITEM = OVERDUE.LINE_ITEM 
	 LEFT JOIN 
	 CLN_PAYMENT_CALENDAR AS CAL ON
	 OI.CLEARING_DATE = CAL.DATES
GO
/****** Object:  StoredProcedure [dbo].[P_TP3_EXEC]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP3_EXEC] AS

EXEC P_TP30_COLUMNS

EXEC P_TP30_IRB

EXEC P_TP30_ALL_ITEMS

GO
/****** Object:  StoredProcedure [dbo].[P_TP30_ALL_ITEMS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP30_ALL_ITEMS] AS

UPDATE TP3_ALL_ITEMS SET KEY_DATE = EOMONTH(SRC_DOWNLOAD_DATE,-1)

UPDATE TP3_ALL_ITEMS SET DUE_DATE = DBO.FC_CALCULATE_DUE_DATE_AP(CONVERT(DATE, BASELINE_DATE,104),CONVERT(INT,DAYS1),CONVERT(INT,DAYS2), DEBIT_CREDIT,FOLLOW_ON_DOC) 

UPDATE TP3_ALL_ITEMS SET ARREARS_AFTER_NET = DBO.FC_CALCULATE_ARREARS(DUE_DATE,KEY_DATE) 

UPDATE TP3_ALL_ITEMS SET TRANSACTION_KEY = CONCAT([YEAR],COMPANY_CODE,DOCUMENT_NUMBER,LINE_ITEM)

UPDATE TP3_ALL_ITEMS SET AMOUNT_LOCAL = AMOUNT_LOCAL * -1 WHERE DEBIT_CREDIT = 'H'

UPDATE TP3_ALL_ITEMS SET AMOUNT_DOCUMENT = AMOUNT_DOCUMENT * -1 WHERE DEBIT_CREDIT = 'H'

UPDATE TP3_ALL_ITEMS SET WHT = 'WITHHOLDING TAX' WHERE LEFT(REFERENCE,3) = 'WHT' OR RIGHT(REFERENCE,3) = 'EIT'  

UPDATE TP3_ALL_ITEMS SET DUPLICATE = 'X' WHERE CHARINDEX('V',REFERENCE) <> 0

GO
/****** Object:  StoredProcedure [dbo].[P_TP30_COLUMNS]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP30_COLUMNS] AS

--COPY TO CALCULATION STAGE

DROP TABLE IF EXISTS TP3_IRB
DROP TABLE IF EXISTS TP3_ALL_ITEMS

SELECT * 
INTO DBO.TP3_IRB
FROM TP2_IRB

SELECT * INTO DBO.TP3_ALL_ITEMS FROM TP2_CLEARED_ITEMS
UNION 
SELECT * FROM TP2_OPEN_ITEMS



IF COL_LENGTH ('DBO.TP3_IRB','SCAN_DATE_TO_INPUT_DATE') IS NULL AND
   COL_LENGTH ('DBO.TP3_IRB','INPUT_DATE_TO_POSTING_DATE') IS NULL
BEGIN
   ALTER TABLE TP3_IRB
   ADD SCAN_DATE_TO_INPUT_DATE VARCHAR(MAX),
   INPUT_DATE_TO_POSTING_DATE VARCHAR(MAX)
END

IF COL_LENGTH ('DBO.TP3_IRB','EIV_AUTOPOST') IS NULL
BEGIN
	ALTER TABLE TP3_IRB
	ADD EIV_AUTOPOST VARCHAR(MAX)
END

IF COL_LENGTH ('DBO.TP3_IRB','KEY_DATE') IS NULL
BEGIN
	ALTER TABLE TP3_IRB
	ADD KEY_DATE DATE
END

IF COL_LENGTH ('DBO.TP3_IRB','AMOUNT_EUR') IS NULL 
BEGIN
	ALTER TABLE TP3_IRB
	ADD AMOUNT_EUR FLOAT 
END


IF COL_LENGTH ('DBO.TP3_ALL_ITEMS','KEY_DATE') IS NULL
BEGIN
	ALTER TABLE TP3_ALL_ITEMS
	ADD KEY_DATE DATE
END

IF COL_LENGTH ('DBO.TP3_ALL_ITEMS','DUE_DATE') IS NULL 
BEGIN
	ALTER TABLE TP3_ALL_ITEMS
	ADD DUE_DATE VARCHAR(MAX)
END 

IF COL_LENGTH ('DBO.TP3_ALL_ITEMS','ARREARS_AFTER_NET') IS NULL 
BEGIN
	ALTER TABLE TP3_ALL_ITEMS
	ADD ARREARS_AFTER_NET VARCHAR(MAX)
END

IF COL_LENGTH ('DBO.TP3_ALL_ITEMS','TRANSACTION_KEY') IS NULL 
BEGIN
	ALTER TABLE TP3_ALL_ITEMS
	ADD TRANSACTION_KEY VARCHAR(MAX)
END

IF COL_LENGTH ('DBO.TP3_ALL_ITEMS','WHT') IS NULL 
BEGIN
	ALTER TABLE TP3_ALL_ITEMS
	ADD WHT VARCHAR(MAX)
END

IF COL_LENGTH ('DBO.TP3_ALL_ITEMS','DUPLICATE') IS NULL 
BEGIN
	ALTER TABLE TP3_ALL_ITEMS
	ADD DUPLICATE VARCHAR(MAX)
END

GO
/****** Object:  StoredProcedure [dbo].[P_TP30_IRB]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP30_IRB] AS 

	UPDATE TP3_IRB SET SCAN_DATE_TO_INPUT_DATE = DBO.FC_GET_BUSINESS_DAYS(SCAN_DATE, CONVERT(DATE, INPUT_DATE,104)) 
	UPDATE TP3_IRB SET INPUT_DATE_TO_POSTING_DATE = DBO.FC_GET_BUSINESS_DAYS(CONVERT(DATE, INPUT_DATE,104), CONVERT(DATE, ENTERED_ON_DATE, 104)) 

	UPDATE TP3_IRB SET EIV_AUTOPOST= 'yes' 
	WHERE INVOICE_INPUT_CHANNEL = 'EIV' AND
		  TS_ERROR_01 IS NULL AND 
		  TS_ERROR_02 IS NULL AND 
		  TS_ERROR_03 IS NULL AND 
		  TS_ERROR_04 IS NULL AND 
		  TS_ERROR_05 IS NULL AND 
		  TS_ERROR_06 IS NULL AND 
		  TS_ERROR_07 IS NULL AND 
		  TS_ERROR_08 IS NULL 

	UPDATE TP3_IRB SET EIV_AUTOPOST = 'no'
	WHERE EIV_AUTOPOST IS NULL AND 
		  INVOICE_INPUT_CHANNEL = 'EIV'

	UPDATE TP3_IRB SET EIV_AUTOPOST= 'N/A'
	WHERE INVOICE_INPUT_CHANNEL <> 'EIV'

UPDATE TP3_IRB SET KEY_DATE = EOMONTH(SRC_DOWNLOAD_DATE,-1)

UPDATE TP3_IRB SET AMOUNT_EUR = O2C.FC_CONVERT_CURRENCY(CURRENCY,CONVERT(DECIMAL(30,2),AMOUNT_DOCUMENT), 'EUR') 
GO
/****** Object:  StoredProcedure [dbo].[P_ZZZ_EXEC_FI5000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_ZZZ_EXEC_FI5000] AS 

DROP TABLE IF EXISTS CLN_FI5000
SELECT * INTO dbo.CLN_FI5000 FROM ING_FI5000

-- FI5000 ---------------------------------------------------------------------
UPDATE CLN_FI5000  SET CASH_DISCOUNT2 = '' WHERE CASH_DISCOUNT2 = '#';
UPDATE CLN_FI5000 SET CLEARING_DATE = '' WHERE CLEARING_DATE = '#';
UPDATE CLN_FI5000 SET ENTERED_ON_DATE = '' WHERE ENTERED_ON_DATE = '#';
UPDATE CLN_FI5000 SET INPUT_DATE = '' WHERE INPUT_DATE = '#';
UPDATE CLN_FI5000 SET INVOICE_DATE = '' WHERE INVOICE_DATE = '#';
UPDATE CLN_FI5000 SET CLEARING_DATE = '' WHERE CLEARING_DATE = '#';
UPDATE CLN_FI5000 SET PAYING_DATE = '' WHERE PAYING_DATE = '#';
UPDATE CLN_FI5000 SET POSTING_DATE = '' WHERE POSTING_DATE = '#';
UPDATE CLN_FI5000 SET SCAN_DATE = '' WHERE SCAN_DATE = '#';
UPDATE CLN_FI5000 SET STATE_AUTO_POSTING = '9' WHERE STATE_AUTO_POSTING = '#';
UPDATE CLN_FI5000 SET OCR_INVOICE_CORRECTION = 'N/A' WHERE OCR_INVOICE_CORRECTION = '#';
UPDATE CLN_FI5000 SET OCR_SUPPLIER_CORRECTION = 'N/A' WHERE OCR_SUPPLIER_CORRECTION = '#';

-- Remove EP1_100 

UPDATE CLN_FI5000 SET FI_DOCUMENT_NO = SUBSTRING(FI_DOCUMENT_NO,9,10)
UPDATE CLN_FI5000 SET PURCHASE_ORDER = SUBSTRING(PURCHASE_ORDER,9,12)
UPDATE CLN_FI5000 SET REFERENCE = SUBSTRING(REFERENCE,9,12)
UPDATE CLN_FI5000 SET VENDOR_NUMBER = SUBSTRING(VENDOR_NUMBER,9,12)
UPDATE CLN_FI5000 SET COMPANY_CODE = SUBSTRING(COMPANY_CODE,9,12)

-- Add leading 0

EXEC o2c.P_EXECUTE_ETL_FUNCTION @IMP_FUNCTION = 'ADD_ZERO' , @IMP_TABLENAME = 'CLN_FI5000', @schema = 'dbo';

-- Calculate Scandate in German Time 

UPDATE CLN_FI5000 SET SCAN_DATE = dbo.FC_GET_SCAN_DATE_GERMAN(CONVERT(date, SCAN_DATE,104),CONVERT(int,SUBSTRING(SCANTIME,1,2)))
GO
/****** Object:  StoredProcedure [dbo].[sp_alterdiagram]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_alterdiagram]
	(
		@diagramname 	sysname,
		@owner_id	int	= null,
		@version 	int,
		@definition 	varbinary(max)
	)
	WITH EXECUTE AS 'dbo'
	AS
	BEGIN
		set nocount on
	
		declare @theId 			int
		declare @retval 		int
		declare @IsDbo 			int
		
		declare @UIDFound 		int
		declare @DiagId			int
		declare @ShouldChangeUID	int
	
		if(@diagramname is null)
		begin
			RAISERROR ('Invalid ARG', 16, 1)
			return -1
		end
	
		execute as caller;
		select @theId = DATABASE_PRINCIPAL_ID();	 
		select @IsDbo = IS_MEMBER(N'db_owner'); 
		if(@owner_id is null)
			select @owner_id = @theId;
		revert;
	
		select @ShouldChangeUID = 0
		select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname 
		
		if(@DiagId IS NULL or (@IsDbo = 0 and @theId <> @UIDFound))
		begin
			RAISERROR ('Diagram does not exist or you do not have permission.', 16, 1);
			return -3
		end
	
		if(@IsDbo <> 0)
		begin
			if(@UIDFound is null or USER_NAME(@UIDFound) is null) -- invalid principal_id
			begin
				select @ShouldChangeUID = 1 ;
			end
		end

		-- update dds data			
		update dbo.sysdiagrams set definition = @definition where diagram_id = @DiagId ;

		-- change owner
		if(@ShouldChangeUID = 1)
			update dbo.sysdiagrams set principal_id = @theId where diagram_id = @DiagId ;

		-- update dds version
		if(@version is not null)
			update dbo.sysdiagrams set version = @version where diagram_id = @DiagId ;

		return 0
	END
	
GO
/****** Object:  StoredProcedure [dbo].[sp_creatediagram]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_creatediagram]
	(
		@diagramname 	sysname,
		@owner_id		int	= null, 	
		@version 		int,
		@definition 	varbinary(max)
	)
	WITH EXECUTE AS 'dbo'
	AS
	BEGIN
		set nocount on
	
		declare @theId int
		declare @retval int
		declare @IsDbo	int
		declare @userName sysname
		if(@version is null or @diagramname is null)
		begin
			RAISERROR (N'E_INVALIDARG', 16, 1);
			return -1
		end
	
		execute as caller;
		select @theId = DATABASE_PRINCIPAL_ID(); 
		select @IsDbo = IS_MEMBER(N'db_owner');
		revert; 
		
		if @owner_id is null
		begin
			select @owner_id = @theId;
		end
		else
		begin
			if @theId <> @owner_id
			begin
				if @IsDbo = 0
				begin
					RAISERROR (N'E_INVALIDARG', 16, 1);
					return -1
				end
				select @theId = @owner_id
			end
		end
		-- next 2 line only for test, will be removed after define name unique
		if EXISTS(select diagram_id from dbo.sysdiagrams where principal_id = @theId and name = @diagramname)
		begin
			RAISERROR ('The name is already used.', 16, 1);
			return -2
		end
	
		insert into dbo.sysdiagrams(name, principal_id , version, definition)
				VALUES(@diagramname, @theId, @version, @definition) ;
		
		select @retval = @@IDENTITY 
		return @retval
	END
	
GO
/****** Object:  StoredProcedure [dbo].[sp_dropdiagram]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_dropdiagram]
	(
		@diagramname 	sysname,
		@owner_id	int	= null
	)
	WITH EXECUTE AS 'dbo'
	AS
	BEGIN
		set nocount on
		declare @theId 			int
		declare @IsDbo 			int
		
		declare @UIDFound 		int
		declare @DiagId			int
	
		if(@diagramname is null)
		begin
			RAISERROR ('Invalid value', 16, 1);
			return -1
		end
	
		EXECUTE AS CALLER;
		select @theId = DATABASE_PRINCIPAL_ID();
		select @IsDbo = IS_MEMBER(N'db_owner'); 
		if(@owner_id is null)
			select @owner_id = @theId;
		REVERT; 
		
		select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname 
		if(@DiagId IS NULL or (@IsDbo = 0 and @UIDFound <> @theId))
		begin
			RAISERROR ('Diagram does not exist or you do not have permission.', 16, 1)
			return -3
		end
	
		delete from dbo.sysdiagrams where diagram_id = @DiagId;
	
		return 0;
	END
	
GO
/****** Object:  StoredProcedure [dbo].[sp_get_dbstat]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2020/01/10
-- Description: get table latest status
-- =============================================
CREATE PROCEDURE [dbo].[sp_get_dbstat]

AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON


-- get DB space used
	SELECT SUM(size/128.0) AS DatabaseDataSpaceAllocatedInMB,
		SUM(size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0)  AS DatabaseDataSpaceAllocatedUnusedInMB,
		sum(CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0) as 'SpaceUsedMB',
		sum(CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0) / (250 * 1024 ) * 100 as 'Used%',
		250 as DatabaseTotalSpaceGB
	FROM sys.database_files
	GROUP BY type_desc
	HAVING type_desc = 'ROWS';


-- get table details
WITH agg AS
(   -- Get info for Tables, Indexed Views, etc
    SELECT  ps.[object_id] AS [ObjectID],
            ps.index_id AS [IndexID],
            NULL AS [ParentIndexID],
            NULL AS [PassThroughIndexName],
            NULL AS [PassThroughIndexType],
            SUM(ps.in_row_data_page_count) AS [InRowDataPageCount],
            SUM(ps.used_page_count) AS [UsedPageCount],
            SUM(ps.reserved_page_count) AS [ReservedPageCount],
            SUM(ps.row_count) AS [RowCount],
            SUM(ps.lob_used_page_count + ps.row_overflow_used_page_count)
                    AS [LobAndRowOverflowUsedPageCount]
    FROM    sys.dm_db_partition_stats ps
    GROUP BY    ps.[object_id],
                ps.[index_id]
    UNION ALL
    -- Get info for FullText indexes, XML indexes, Spatial indexes, etc
    SELECT  sit.[parent_id] AS [ObjectID],
            sit.[object_id] AS [IndexID],
            sit.[parent_minor_id] AS [ParentIndexID],
            sit.[name] AS [PassThroughIndexName],
            sit.[internal_type_desc] AS [PassThroughIndexType],
            0 AS [InRowDataPageCount],
            SUM(ps.used_page_count) AS [UsedPageCount],
            SUM(ps.reserved_page_count) AS [ReservedPageCount],
            0 AS [RowCount],
            0 AS [LobAndRowOverflowUsedPageCount]
    FROM    sys.dm_db_partition_stats ps
    INNER JOIN  sys.internal_tables sit
            ON  sit.[object_id] = ps.[object_id]
    WHERE   sit.internal_type IN
               (202, 204, 207, 211, 212, 213, 214, 215, 216, 221, 222, 236)
    GROUP BY    sit.[parent_id],
                sit.[object_id],
                sit.[parent_minor_id],
                sit.[name],
                sit.[internal_type_desc]
), spaceused AS
(
SELECT  agg.[ObjectID],
        agg.[IndexID],
        agg.[ParentIndexID],
        agg.[PassThroughIndexName],
        agg.[PassThroughIndexType],
        OBJECT_SCHEMA_NAME(agg.[ObjectID]) AS [SchemaName],
        OBJECT_NAME(agg.[ObjectID]) AS [TableName],
        SUM(CASE
                WHEN (agg.IndexID < 2) THEN agg.[RowCount]
                ELSE 0
            END) AS [Rows],
        SUM(agg.ReservedPageCount) * 8 AS [ReservedKB],
        SUM(agg.LobAndRowOverflowUsedPageCount +
            CASE
                WHEN (agg.IndexID < 2) THEN (agg.InRowDataPageCount)
                ELSE 0
            END) * 8 AS [DataKB],
        SUM(agg.UsedPageCount - agg.LobAndRowOverflowUsedPageCount -
            CASE
                WHEN (agg.IndexID < 2) THEN agg.InRowDataPageCount
                ELSE 0
            END) * 8 AS [IndexKB],
        SUM(agg.ReservedPageCount - agg.UsedPageCount) * 8 AS [UnusedKB],
        SUM(agg.UsedPageCount) * 8 AS [UsedKB]
FROM    agg
GROUP BY    agg.[ObjectID],
            agg.[IndexID],
            agg.[ParentIndexID],
            agg.[PassThroughIndexName],
            agg.[PassThroughIndexType],
            OBJECT_SCHEMA_NAME(agg.[ObjectID]),
            OBJECT_NAME(agg.[ObjectID])
)

SELECT sp.SchemaName,
       sp.TableName,
       sp.IndexID,
       CASE
         WHEN (sp.IndexID > 0) THEN COALESCE(si.[name], sp.[PassThroughIndexName])
         ELSE N'<Heap>'
       END AS [IndexName],
       sp.[PassThroughIndexName] AS [InternalTableName],
       sp.[Rows],
       sp.ReservedKB,
       (sp.ReservedKB / 1024.0 / 1024.0) AS [ReservedGB],
       sp.DataKB,
       (sp.DataKB / 1024.0 / 1024.0) AS [DataGB],
       sp.IndexKB,
       (sp.IndexKB / 1024.0 / 1024.0) AS [IndexGB],
       sp.UsedKB AS [UsedKB],
       (sp.UsedKB / 1024.0 / 1024.0) AS [UsedGB],
       sp.UnusedKB,
       (sp.UnusedKB / 1024.0 / 1024.0) AS [UnusedGB],
       so.[type_desc] AS [ObjectType],
       COALESCE(si.type_desc, sp.[PassThroughIndexType]) AS [IndexPrimaryType],
       sp.[PassThroughIndexType] AS [IndexSecondaryType],
       SCHEMA_ID(sp.[SchemaName]) AS [SchemaID],
       sp.ObjectID
       --,sp.ParentIndexID
FROM   spaceused sp
INNER JOIN sys.all_objects so -- in case "WHERE so.is_ms_shipped = 0" is removed
        ON so.[object_id] = sp.ObjectID
LEFT JOIN  sys.indexes si
       ON  si.[object_id] = sp.ObjectID
      AND  (si.[index_id] = sp.IndexID
         OR si.[index_id] = sp.[ParentIndexID])
WHERE so.is_ms_shipped = 0
END

GO
/****** Object:  StoredProcedure [dbo].[sp_helpdiagramdefinition]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_helpdiagramdefinition]
	(
		@diagramname 	sysname,
		@owner_id	int	= null 		
	)
	WITH EXECUTE AS N'dbo'
	AS
	BEGIN
		set nocount on

		declare @theId 		int
		declare @IsDbo 		int
		declare @DiagId		int
		declare @UIDFound	int
	
		if(@diagramname is null)
		begin
			RAISERROR (N'E_INVALIDARG', 16, 1);
			return -1
		end
	
		execute as caller;
		select @theId = DATABASE_PRINCIPAL_ID();
		select @IsDbo = IS_MEMBER(N'db_owner');
		if(@owner_id is null)
			select @owner_id = @theId;
		revert; 
	
		select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname;
		if(@DiagId IS NULL or (@IsDbo = 0 and @UIDFound <> @theId ))
		begin
			RAISERROR ('Diagram does not exist or you do not have permission.', 16, 1);
			return -3
		end

		select version, definition FROM dbo.sysdiagrams where diagram_id = @DiagId ; 
		return 0
	END
	
GO
/****** Object:  StoredProcedure [dbo].[sp_helpdiagrams]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_helpdiagrams]
	(
		@diagramname sysname = NULL,
		@owner_id int = NULL
	)
	WITH EXECUTE AS N'dbo'
	AS
	BEGIN
		DECLARE @user sysname
		DECLARE @dboLogin bit
		EXECUTE AS CALLER;
			SET @user = USER_NAME();
			SET @dboLogin = CONVERT(bit,IS_MEMBER('db_owner'));
		REVERT;
		SELECT
			[Database] = DB_NAME(),
			[Name] = name,
			[ID] = diagram_id,
			[Owner] = USER_NAME(principal_id),
			[OwnerID] = principal_id
		FROM
			sysdiagrams
		WHERE
			(@dboLogin = 1 OR USER_NAME(principal_id) = @user) AND
			(@diagramname IS NULL OR name = @diagramname) AND
			(@owner_id IS NULL OR principal_id = @owner_id)
		ORDER BY
			4, 5, 1
	END
	
GO
/****** Object:  StoredProcedure [dbo].[sp_renamediagram]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_renamediagram]
	(
		@diagramname 		sysname,
		@owner_id		int	= null,
		@new_diagramname	sysname
	
	)
	WITH EXECUTE AS 'dbo'
	AS
	BEGIN
		set nocount on
		declare @theId 			int
		declare @IsDbo 			int
		
		declare @UIDFound 		int
		declare @DiagId			int
		declare @DiagIdTarg		int
		declare @u_name			sysname
		if((@diagramname is null) or (@new_diagramname is null))
		begin
			RAISERROR ('Invalid value', 16, 1);
			return -1
		end
	
		EXECUTE AS CALLER;
		select @theId = DATABASE_PRINCIPAL_ID();
		select @IsDbo = IS_MEMBER(N'db_owner'); 
		if(@owner_id is null)
			select @owner_id = @theId;
		REVERT;
	
		select @u_name = USER_NAME(@owner_id)
	
		select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname 
		if(@DiagId IS NULL or (@IsDbo = 0 and @UIDFound <> @theId))
		begin
			RAISERROR ('Diagram does not exist or you do not have permission.', 16, 1)
			return -3
		end
	
		-- if((@u_name is not null) and (@new_diagramname = @diagramname))	-- nothing will change
		--	return 0;
	
		if(@u_name is null)
			select @DiagIdTarg = diagram_id from dbo.sysdiagrams where principal_id = @theId and name = @new_diagramname
		else
			select @DiagIdTarg = diagram_id from dbo.sysdiagrams where principal_id = @owner_id and name = @new_diagramname
	
		if((@DiagIdTarg is not null) and  @DiagId <> @DiagIdTarg)
		begin
			RAISERROR ('The name is already used.', 16, 1);
			return -2
		end		
	
		if(@u_name is null)
			update dbo.sysdiagrams set [name] = @new_diagramname, principal_id = @theId where diagram_id = @DiagId
		else
			update dbo.sysdiagrams set [name] = @new_diagramname where diagram_id = @DiagId
		return 0
	END
	
GO
/****** Object:  StoredProcedure [dbo].[sp_upgraddiagrams]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_upgraddiagrams]
	AS
	BEGIN
		IF OBJECT_ID(N'dbo.sysdiagrams') IS NOT NULL
			return 0;
	
		CREATE TABLE dbo.sysdiagrams
		(
			name sysname NOT NULL,
			principal_id int NOT NULL,	-- we may change it to varbinary(85)
			diagram_id int PRIMARY KEY IDENTITY,
			version int,
	
			definition varbinary(max)
			CONSTRAINT UK_principal_name UNIQUE
			(
				principal_id,
				name
			)
		);


		/* Add this if we need to have some form of extended properties for diagrams */
		/*
		IF OBJECT_ID(N'dbo.sysdiagram_properties') IS NULL
		BEGIN
			CREATE TABLE dbo.sysdiagram_properties
			(
				diagram_id int,
				name sysname,
				value varbinary(max) NOT NULL
			)
		END
		*/

		IF OBJECT_ID(N'dbo.dtproperties') IS NOT NULL
		begin
			insert into dbo.sysdiagrams
			(
				[name],
				[principal_id],
				[version],
				[definition]
			)
			select	 
				convert(sysname, dgnm.[uvalue]),
				DATABASE_PRINCIPAL_ID(N'dbo'),			-- will change to the sid of sa
				0,							-- zero for old format, dgdef.[version],
				dgdef.[lvalue]
			from dbo.[dtproperties] dgnm
				inner join dbo.[dtproperties] dggd on dggd.[property] = 'DtgSchemaGUID' and dggd.[objectid] = dgnm.[objectid]	
				inner join dbo.[dtproperties] dgdef on dgdef.[property] = 'DtgSchemaDATA' and dgdef.[objectid] = dgnm.[objectid]
				
			where dgnm.[property] = 'DtgSchemaNAME' and dggd.[uvalue] like N'_EA3E6268-D998-11CE-9454-00AA00A3F36E_' 
			return 2;
		end
		return 1;
	END
	
GO
/****** Object:  StoredProcedure [dbo].[update_metadata]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2020-12-24
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [dbo].[update_metadata]

AS
BEGIN

   drop table if exists dbo.metadata


	select TableName, count(*) as ColCount into #tempcol from 
	(
	select (SCHEMA_NAME(t.schema_id) + '.' + t.Name) as TableName, t.object_id, c.name as colname,c.column_id as colid ,
	t.type_desc, t.create_date  from sys.objects as t
	left outer join sys.all_columns as c
	on t.object_id = c.object_id
	where t.type = 'U' and t.schema_id <> 4

	) M
	group by M.TableName
	order by M.TableName

	--select * from #tempcol
	-- drop table #tempcol 


	-- get row count of each table

	SELECT  (SCHEMA_NAME(A.schema_id) + '.' + A.Name) AS TableName  
	, SUM(B.rows) AS RowsCount  into #temprow
	FROM sys.objects A  
	INNER JOIN sys.partitions B ON A.object_id = B.object_id  
	WHERE A.type = 'U'  and A.schema_id <> 4
	GROUP BY A.schema_id, A.Name  

	--select * from #temprow
	--drop table #temprow


	select a.tablename, a.rowscount, b.colcount into dbo.metadata from #temprow as a 
	left outer join #tempcol as b 


	on a.TableName = b.TableName
	order by tablename 

	drop table #temprow 
	drop table #tempcol


END
GO
/****** Object:  StoredProcedure [o2c].[p_cln_adrc]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_adrc] as

update o2c.cln_adrc
set o2c.cln_adrc.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name <> 'adrc'

delete from o2c.cln_adrc
where file_path <> (select max(file_path) from o2c.cln_adrc)



GO
/****** Object:  StoredProcedure [o2c].[p_cln_bkpf]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_bkpf] as

delete from o2c.cln_bkpf
where file_path <> (select max(file_path) from o2c.cln_bkpf)

update o2c.cln_bkpf
set o2c.cln_bkpf.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'BKPF_FSSC_improvement_Framework'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_bsad]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_bsad] as

delete from o2c.cln_bsad
where file_path <> (select max(file_path) from o2c.cln_bsad)

update o2c.cln_bsad
set o2c.cln_bsad.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'BSAD'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_bsid]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_bsid] as

delete from o2c.cln_bsid
where file_path <> (select max(file_path) from o2c.cln_bsid)

update o2c.cln_bsid
set o2c.cln_bsid.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'BSID'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_clean_columns]    Script Date: 4/8/2021 1:59:59 PM ******/
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
/****** Object:  StoredProcedure [o2c].[p_cln_eflow_likp]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_eflow_likp] AS

-- move content from ing data to cln table
truncate table o2c.cln_eflowtask
insert o2c.cln_eflowtask
select distinct 
	   left([task_id],50) [task_id]
	   ,left([processname],20) [processname]
      ,[incident]
      ,left([steplabel],30) [steplabel]
      ,left([taskuser],40) [taskuser]
      ,left([assignedtouser],40) [assignedtouser]
      ,[status]
      ,[substatus]
      ,[starttime]
      ,[endtime]
      ,[download_date]  
  from [o2c].[ing_eflowtask]
  where processname = 'P047_CLR_01' or ( processname = 'P048_GR_01' and steplabel = 'FI Release' )
        and endtime >='2020-01-01'

truncate table o2c.cln_eflowdn
insert o2c.cln_eflowdn
select distinct left(processname,20) processname,incident , left(dntbr,20) dntbr,   download_date from o2c.ing_eflowdn
 


GO
/****** Object:  StoredProcedure [o2c].[p_cln_exec]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_exec] @schema varchar(max) AS

-- FIRST---------------------------------------------------------------------

exec o2c.p_cln_first @schema = 'o2c'

exec o2c.p_cln_src_download_date @schema = 'o2c'

exec o2c.p_cln_load_details
------

exec o2c.p_cln_adrc

exec o2c.p_cln_bkpf

exec o2c.p_cln_bsad

exec o2c.p_cln_bsid

exec o2c.p_cln_fdm_dcproc

exec o2c.p_cln_kna1

exec o2c.p_cln_knb1

exec o2c.p_cln_knkk

exec o2c.p_cln_scmg_t_case_attr

exec o2c.p_cln_t001

exec o2c.p_cln_t001s

exec o2c.p_cln_t052

exec o2c.p_cln_udmcaseattr00

exec o2c.p_cln_eflow_likp

exec o2c.p_cln_likp

exec o2c.p_cln_vbuk

exec o2c.p_cln_fi1000

exec o2c.p_cln_sample_orders

--LAST   ---------------------------------------------------------------------------

exec o2c.p_cln_clean_columns @schema = 'o2c'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_fdm_dcproc]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_fdm_dcproc] as

delete from o2c.cln_fdm_dcproc
where file_path <> (select max(file_path) from o2c.cln_fdm_dcproc);

with duplicates as (
    select *,
        row_number() over (
            partition by 
                transaction_key
		    order by 
		        transaction_key
        ) row_num
     from 
        o2c.cln_fdm_dcproc
)

delete from duplicates
where row_num > 1

update o2c.cln_fdm_dcproc
set o2c.cln_fdm_dcproc.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'FDM_DCPROC'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_cln_fi1000] as 


declare @inyearmonth nvarchar(10)
declare @fulldate nvarchar(10)
declare @initdate date
select @inyearmonth = max(monthyearfrom) from o2c.ing_fi1000 
select @fulldate = concat(right(@inyearmonth,4),left(@inyearmonth,2),'01')
select @initdate = convert(date,@fulldate,102)

drop table if exists o2c.#bwfi1000ar
select company_code, customer_number,business_division,business_unit,credit_control_area,division,
    case yearmonth 
     when 'ar_month_0' then @initdate
	 when 'ar_month_1' then dateadd(MONTH,-1,@initdate)
	 when 'ar_month_2' then dateadd(MONTH,-2,@initdate)
	 when 'ar_month_3' then dateadd(MONTH,-3,@initdate)
	 when 'ar_month_4' then dateadd(MONTH,-4,@initdate)
	 when 'ar_month_5' then dateadd(MONTH,-5,@initdate)
	 when 'ar_month_6' then dateadd(MONTH,-6,@initdate)
	 when 'ar_month_7' then dateadd(MONTH,-7,@initdate)
	 when 'ar_month_8' then dateadd(MONTH,-8,@initdate)
	 when 'ar_month_9' then dateadd(MONTH,-9,@initdate)
	 when 'ar_month_10' then dateadd(MONTH,-10,@initdate)
	 when 'ar_month_11' then dateadd(MONTH,-11,@initdate)
	 when 'ar_month_12' then dateadd(MONTH,-12,@initdate)
   end as postdate, 'aramount' as category, amount
 into o2c.#bwfi1000ar
 from 
(
	select  company_code,customer_number,business_division,business_unit,credit_control_area,division,ar_month_0 , ar_month_1,ar_month_2,
	      ar_month_3,ar_month_4,ar_month_5,ar_month_6,ar_month_7,ar_month_8,ar_month_9,ar_month_10,ar_month_11,ar_month_12
	from o2c.ing_fi1000
)  p

unpivot (
    amount for yearmonth in (ar_month_0,ar_month_1,ar_month_2,ar_month_3,ar_month_4,ar_month_5,ar_month_6,ar_month_7,ar_month_8,ar_month_9,ar_month_10,ar_month_11,ar_month_12)
	)
 as unpvtar;



drop table if exists o2c.#bwfi1000sales
select company_code, customer_number,business_division,business_unit,credit_control_area,division,
    case yearmonth 
     when 'sales_month_0' then @initdate
	 when 'sales_month_1' then dateadd(MONTH,-1,@initdate)
	 when 'sales_month_2' then dateadd(MONTH,-2,@initdate)
	 when 'sales_month_3' then dateadd(MONTH,-3,@initdate)
	 when 'sales_month_4' then dateadd(MONTH,-4,@initdate)
	 when 'sales_month_5' then dateadd(MONTH,-5,@initdate)
	 when 'sales_month_6' then dateadd(MONTH,-6,@initdate)
	 when 'sales_month_7' then dateadd(MONTH,-7,@initdate)
	 when 'sales_month_8' then dateadd(MONTH,-8,@initdate)
	 when 'sales_month_9' then dateadd(MONTH,-9,@initdate)
	 when 'sales_month_10' then dateadd(MONTH,-10,@initdate)
	 when 'sales_month_11' then dateadd(MONTH,-11,@initdate)
	 when 'sales_month_12' then dateadd(MONTH,-12,@initdate)
   end as postdate, 'salesamount' as category,amount
 into o2c.#bwfi1000sales
 from 
(
	select  company_code,customer_number,business_division,business_unit,credit_control_area,division,sales_month_0 , sales_month_1,sales_month_2,
	      sales_month_3,sales_month_4,sales_month_5,sales_month_6,sales_month_7,sales_month_8,sales_month_9,sales_month_10,sales_month_11,sales_month_12
	from o2c.ing_fi1000
)  p

unpivot (
    amount for yearmonth in (sales_month_0,sales_month_1,sales_month_2,sales_month_3,sales_month_4,sales_month_5,sales_month_6,sales_month_7,sales_month_8,sales_month_9,sales_month_10,sales_month_11,sales_month_12)
	)
 as unpvtsales;



 -- unpivot overdue amount
drop table if exists o2c.#bwfi1000overdue
select company_code, customer_number,business_division,business_unit,credit_control_area,division,
    case yearmonth 
     when 'overdue_month_0' then @initdate
	 when 'overdue_month_1' then dateadd(MONTH,-1,@initdate)
	 when 'overdue_month_2' then dateadd(MONTH,-2,@initdate)
	 when 'overdue_month_3' then dateadd(MONTH,-3,@initdate)
	 when 'overdue_month_4' then dateadd(MONTH,-4,@initdate)
	 when 'overdue_month_5' then dateadd(MONTH,-5,@initdate)
	 when 'overdue_month_6' then dateadd(MONTH,-6,@initdate)
	 when 'overdue_month_7' then dateadd(MONTH,-7,@initdate)
	 when 'overdue_month_8' then dateadd(MONTH,-8,@initdate)
	 when 'overdue_month_9' then dateadd(MONTH,-9,@initdate)
	 when 'overdue_month_10' then dateadd(MONTH,-10,@initdate)
	 when 'overdue_month_11' then dateadd(MONTH,-11,@initdate)
	 when 'overdue_month_12' then dateadd(MONTH,-12,@initdate)
   end as postdate, 'overdueamount' as category, amount
 into o2c.#bwfi1000overdue
 from 
(
	select  company_code,customer_number,business_division,business_unit,credit_control_area,division,
	overdue_month_0 , overdue_month_1,overdue_month_2,overdue_month_3,overdue_month_4,overdue_month_5,overdue_month_6,overdue_month_7,overdue_month_8,
	overdue_month_9,overdue_month_10,overdue_month_11,overdue_month_12
	from o2c.ing_fi1000
)  p

unpivot (
    amount for yearmonth in (overdue_month_0 , overdue_month_1,overdue_month_2,overdue_month_3,overdue_month_4,overdue_month_5,overdue_month_6,
	   overdue_month_7,overdue_month_8,overdue_month_9,overdue_month_10,overdue_month_11,overdue_month_12)
	)
 as unpvtoverdue;






 drop table if exists o2c.#bwfi1000
 select *  into o2c.#bwfi1000 from o2c.#bwfi1000ar
 union all 
 select * from o2c.#bwfi1000sales
 union all 
 select * from o2c.#bwfi1000overdue


 --pivot 
 drop table if exists o2c.cln_fi1000
 select company_code,customer_number,business_division,business_unit,credit_control_area,division,postdate, 
 isnull(aramount,0) aramount ,isnull(salesamount,0) salesamount ,isnull(overdueamount,0) overdueamount
 into o2c.cln_fi1000
 from 
 (
 select company_code,customer_number,business_division,business_unit,credit_control_area,division,postdate,category,amount from o2c.#bwfi1000
 )p
  pivot 
 (
    sum(amount) for category in (aramount,salesamount,overdueamount) 
 ) as pvt

  order by  postdate

   drop table if exists o2c.#bwfi1000ar
   drop table if exists o2c.#bwfi1000sales
   drop table if exists o2c.#bwfi1000overdue
   drop table if exists o2c.#bwfi1000


GO
/****** Object:  StoredProcedure [o2c].[p_cln_first]    Script Date: 4/8/2021 1:59:59 PM ******/
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
/****** Object:  StoredProcedure [o2c].[p_cln_kna1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_kna1] as

delete from o2c.cln_kna1
where file_path <> (select max(file_path) from o2c.cln_kna1)

update o2c.cln_kna1
set o2c.cln_kna1.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'KNA1'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_knb1]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_knb1] as

delete from o2c.cln_knb1
where file_path <> (select max(file_path) from o2c.cln_knb1)

update o2c.cln_knb1
set o2c.cln_knb1.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'KNB1'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_knkk]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_knkk] as

delete from o2c.cln_knkk
where file_path <> (select max(file_path) from o2c.cln_knkk)

update o2c.cln_knkk
set o2c.cln_knkk.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'KNKK'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_likp]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2020/03/02
-- Description: data cleaning for delivery LIKP table
-- =============================================
CREATE PROCEDURE [o2c].[p_cln_likp]

AS
BEGIN

    insert o2c.cln_likp
	select  [delivery_nr],[created_by],	[created_on],[shippoint] ,[salesorg],
			[delivery_type], [delivery_date],[billing_block],[soldtoparty],	[rel_cre_value],
			[rel_cre_date],	[actual_gi_date], [file_path], [download_date], null
	 from o2c.ing_likp
	 where file_path in (select max(file_path) from o2c.ing_likp)
	

	if col_length ('o2c.cln_likp','durationmin_task_id') is null
	begin
	alter table o2c.cln_likp
	add durationmin_task_id int
	end

	update o2c.cln_likp
	 set o2c.cln_likp.src_download_date  = src.src_download_date
	 from o2c.cln_load_details as src
	 where table_name = 'LIKP'
END

GO
/****** Object:  StoredProcedure [o2c].[p_cln_load_details]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [o2c].[p_cln_load_details] as

delete from o2c.cln_load_details
where file_path <> (select max(file_path) from o2c.cln_load_details)

update o2c.cln_load_details set table_name = substring(table_name,5,len(table_name))
GO
/****** Object:  StoredProcedure [o2c].[p_cln_sample_orders]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_sample_orders] AS



drop table if exists o2c.cln_sample_orders;
select * into o2c.cln_sample_orders from o2c.inx_sample_orders;

with overdue_duplicates as (
    select *,
        row_number() over (
            partition by 
                document_number,
				company_code, 
				[year], 
				line_item
            order by 
				document_number,
				company_code, 
				[year], 
				line_item, 
				key_date desc
        ) row_num
     from 
       o2c.cln_sample_orders
)

delete from overdue_duplicates
where row_num > 1 



exec o2c.p_execute_etl_function @imp_function = 'ADD_ZERO' , @imp_tablename = 'cln_sample_orders' ,@schema  = 'o2c'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_scmg_t_case_attr]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_scmg_t_case_attr] as

delete from o2c.cln_scmg_t_case_attr
where file_path <> (select max(file_path) from o2c.cln_scmg_t_case_attr)

update o2c.cln_scmg_t_case_attr
set o2c.cln_scmg_t_case_attr.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'scmg_t_case_attr'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_src_download_date]    Script Date: 4/8/2021 1:59:59 PM ******/
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
/****** Object:  StoredProcedure [o2c].[p_cln_t001]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_t001] as

delete from o2c.cln_t001
where file_path <> (select max(file_path) from o2c.cln_t001)

update o2c.cln_t001
set o2c.cln_t001.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 't001'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_t001s]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_t001s] as

delete from o2c.cln_t001s
where file_path <> (select max(file_path) from o2c.cln_t001s);

with t001s_duplicates as (
    select *,
        row_number() over (
            partition by 
                company_code, 
                accounting_clerk_number
		    order by 
		        company_code,
				 accounting_clerk_number
        ) row_num
     from 
        o2c.cln_t001s
)

delete from t001s_duplicates
where row_num > 1

update o2c.cln_t001s
set o2c.cln_t001s.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 't001s'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_t052]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create PROCEDURE [o2c].[p_cln_t052] as

delete from o2c.cln_t052
where file_path <> (select max(file_path) from o2c.cln_t052)

update o2c.cln_t052
set o2c.cln_t052.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 't052'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_udmcaseattr00]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_udmcaseattr00] as

delete from o2c.cln_udmcaseattr00
where file_path <> (select max(file_path) from o2c.cln_udmcaseattr00)

update o2c.cln_udmcaseattr00
set o2c.cln_udmcaseattr00.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'udmcaseattr00'
GO
/****** Object:  StoredProcedure [o2c].[p_cln_vbuk]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2020/03/02
-- Description: <clean table vbuk
-- =============================================
CREATE PROCEDURE [o2c].[p_cln_vbuk]
AS
BEGIN
--recreate new cleaning VBUK table with index
	drop table if exists o2c.cln_vbuk 
	create table [o2c].[cln_vbuk](
		[delivery_nr] [nvarchar](10) not null primary key,
		[delivery_status] [nvarchar](1) null,
		[gi_status] [nvarchar](1) null,
		[billing_status] [nvarchar](1) null,
		[file_path] [nvarchar](100),
		[download_date] [datetime] null,
		[src_download_date] [nvarchar](max) null
	) 

	insert o2c.cln_vbuk 
	select  [delivery_nr],[delivery_status],[gi_status],[billing_status],[file_path], [download_date], null from o2c.ing_vbuk
	 where file_path in (select max(file_path) from o2c.ing_vbuk)

	update o2c.cln_vbuk
set o2c.cln_vbuk.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'VBUK'
	END
GO
/****** Object:  StoredProcedure [o2c].[p_execute_etl_function]    Script Date: 4/8/2021 1:59:59 PM ******/
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
/****** Object:  StoredProcedure [o2c].[p_sta_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_all_cust_items] as

drop table if exists o2c.sta_all_cust_items

declare @max_clearing_date as date 

set @max_clearing_date = (select eomonth(max(clearing_date)) from o2c.tp3_all_cust_items)

select 
company_code,
document_number,
fiscal_year,
line_item,
customer_number,
special_gl_indicator,
clearing_date,
clearing_document,
assignment_number,
posting_date,
document_date,
entry_date,
currency,
reference,
document_type,
posting_key,
debit_credit,
amount_local,
amount_tax,
item_text,
gl_account,
baseline_date,
payment_terms,
payment_block,
follow_on_doc,
dunning_block,
dunning_key,
dunning_date_last,
dunning_level,
dunning_area,
billing_document,
credit_control_area,
days1,
days2,
days3,
reference_key1,
file_path,
transaction_key,
download_date,
src_download_date,
document_posted_by,
accounting_clerk,
reconciliation_account,
customer_country,
customer_name1,
customer_name2,
trading_partner,
accounting_clerk_name,
accounting_clerk_user,
customer_name_chinese,
credit_limit,
credit_account,
risk_category,
credit_block,
last_internal_review,
credit_reporting_group,
dispute_object_type,
dispute_coordinator,
dispute_process_deadline,
dispute_detailed_cause,
dispute_case_id,
dispute_case_type,
dispute_case_title,
dispute_planned_close_date,
dispute_reason,
dispute_status,
dispute_responsible,
dispute_processor,
dispute_created_on,
dispute_closed_on,
dispute_changed_on,
key_date,
due_date,
arrears_after_net,
amount_eur,
due_date_vat,
arrears_after_net_vat,
overdue_rank,
overdue_value,
relevant_for_payment_behavior,
amount_document, 
posting_to_clearing_days, 
vat_issued, 
reason,
reason_details, 
days1_vat, 
days2_vat,
reverse_document, 
overdue_rank_vat,
sample_order
into o2c.sta_all_cust_items
from o2c.tp3_all_cust_items

delete from o2c.sta_all_cust_items where 
clearing_date <= dateadd(month,-1,dateadd(year,-1,@max_clearing_date)) and clearing_date is not null 

exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_all_cust_items', @schema = 'o2c'
GO
/****** Object:  StoredProcedure [o2c].[p_sta_create_schema_tables]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_create_schema_tables] as 

drop table if exists o2c.sta_all_cust_items_schema
select top(500)* into o2c.sta_all_cust_items_schema from o2c.sta_all_cust_items
where reference_key1 <> ''

drop table if exists o2c.sta_open_cust_items_schema
select top(500)* into o2c.sta_open_cust_items_schema from o2c.sta_open_cust_items
where reference_key1 <> ''

drop table if exists o2c.sta_eflow_clr_schema
select top(500)* into o2c.sta_eflow_clr_schema from o2c.sta_eflow_clr

drop table if exists o2c.sta_eflow_likp_schema
select top(500)* into o2c.sta_eflow_likp_schema from o2c.sta_eflow_likp

drop table if exists o2c.sta_fi1000_schema
select top(500)* into o2c.sta_fi1000_schema from o2c.sta_fi1000

drop table if exists o2c.sta_payment_behavior_schema
select top(500)* into o2c.sta_payment_behavior_schema from o2c.sta_payment_behavior
GO
/****** Object:  StoredProcedure [o2c].[p_sta_eflow_clr]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [o2c].[p_sta_eflow_clr]
AS
BEGIN
--Statisit table eflowclr old version , dont consider holiday 
/**
	drop table if exists o2c.sta_eflow_clr
	select processname,incident,steplabel,status, 
	  case 
	     when status = 1 then 'open'
		 when status =3 then 'Complete'
		 when status = 4 then 'Return'
		 when status = 7 then 'Rejected'
	  end as StatusText,
	  substatus ,taskuser, assignedtouser,starttime,endtime,task_id,
	  DATEDIFF(minute,STARTTIME,ENDTIME) as 'durationmin' 
	  into o2c.sta_eflow_clr
	 from o2c.cln_eflowtask
	 where processname = 'P047_CLR_01'   
	   and endtime >='2020-01-01'
**/

-- New version consider the payment calendar
  drop table if exists o2c.sta_eflow_clr
	select processname,incident,steplabel,status, 
	  case 
	     when status = 1 then 'open'
		 when status =3 then 'Complete'
		 when status = 4 then 'Return'
		 when status = 7 then 'Rejected'
	  end as StatusText,
	  substatus ,taskuser, assignedtouser,starttime,endtime,task_id,
	  o2c.fc_cal_eflow_duration(starttime,endtime) as 'durationmin' 
	  into o2c.sta_eflow_clr
	 from o2c.cln_eflowtask
	 where processname = 'P047_CLR_01'   
	   and endtime >='2020-01-01'

END
GO
/****** Object:  StoredProcedure [o2c].[p_sta_eflow_likp]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_sta_eflow_likp] AS

-- create temperary table for transforming...

drop table if exists o2c.#cln_dnsum
select  a.PROCESSNAME,a.INCIDENT,right(concat('00000',DNTBR),10) as DELIVERY_NR ,
b.task_id, b.STEPLABEL, b.status, b.substatus, 
	   case
	     when b.status = 1 then 'Open'
		 when b.status = 3 then 'Complete'
		 when b.status = 4 then 'Return'
		 when b.status = 7 then 'Rejected'
	   end as StatusText,
	   b.starttime, b.endtime,
	   case 
	    when b.status = 7 then 0
		else  o2c.fc_cal_eflow_duration(b.starttime,b.endtime)  
	   end as  'DURATIONMIN' 
into o2c.#cln_dnsum
from o2c.cln_eflowdn as a 
left outer join o2c.cln_eflowtask  as b 
on a.PROCESSNAME = b.PROCESSNAME and a.INCIDENT = b.INCIDENT 
where b.processname = 'P048_GR_01' and b.endtime >='2020-01-01'

-- delete temperary table 


delete from o2c.#cln_dnsum where task_id is null ;

with dn_duplicates as (
    select *,
        row_number() over (
            partition by 
                delivery_nr
		    order by 
		        delivery_nr,
				convert(datetime,starttime) desc
        ) row_num
     from 
        o2c.#cln_dnsum
)
delete from dn_duplicates
where row_num > 1

drop table if exists o2c.sta_eflow_likp
select a.*,
	   d.customer_country,
	   d.trading_partner,
	   d.customer_name1,
	   d.customer_name2,
       c.delivery_status, 
	   c.gi_status, 
	   c.billing_status,
	   b.status,
	   b.statustext,
	   b.substatus,
	   b.task_id,
	isnull(durationmin,-1) durationmin
into o2c.sta_eflow_likp
from o2c.cln_likp a
left outer join o2c.#cln_dnsum b
on a.delivery_nr = b.delivery_nr  and 
convert(date,a.rel_cre_date) = convert(date,b.starttime)
left outer join o2c.ing_vbuk c
on a.delivery_nr = c.delivery_nr
left outer join o2c.cln_kna1 d 
 on a.SOLDTOPARTY = d.customer_number
order by durationmin desc

drop table if exists o2c.#cln_dnsum

exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_eflow_likp', @schema = 'o2c';

WITH measure_based_on_task_id AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                task_id
            ORDER BY 
				task_id
        ) row_num
     FROM 
        o2c.sta_eflow_likp
)

update o2c.sta_eflow_likp set o2c.sta_eflow_likp.durationmin_task_id = measure_based_on_task_id.durationmin
from measure_based_on_task_id
where measure_based_on_task_id.delivery_nr = o2c.sta_eflow_likp.delivery_nr and 
row_num = 1

GO
/****** Object:  StoredProcedure [o2c].[p_sta_exec]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_exec] as 

exec o2c.p_sta_open_cust_items 

exec o2c.p_sta_all_cust_items 

exec o2c.p_sta_eflow_clr 

exec o2c.p_sta_eflow_likp

exec o2c.p_sta_fi1000

exec o2c.p_sta_payment_behavior

exec o2c.p_sta_create_schema_tables
GO
/****** Object:  StoredProcedure [o2c].[p_sta_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2021/03/12
-- Description: combine bwfi1000 report and customer master data 
-- =============================================
CREATE PROCEDURE [o2c].[p_sta_fi1000]
AS
BEGIN

	drop  table if exists o2c.sta_fi1000;

	select * 
	into o2c.sta_fi1000
	from o2c.tp3_fi1000

	exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_fi1000', @schema = 'o2c'

end
GO
/****** Object:  StoredProcedure [o2c].[p_sta_open_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [o2c].[p_sta_open_cust_items] as

declare @keydate_tp3 date
declare @month int 
set @month = 0 

set @keydate_tp3 = (select max(key_date) from o2c.tp3_all_cust_items)

drop table if exists o2c.sta_open_cust_items

select 
company_code,
document_number,
fiscal_year,
line_item,
customer_number,
special_gl_indicator,
clearing_date,
clearing_document,
assignment_number,
posting_date,
document_date,
entry_date,
currency,
reference,
document_type,
posting_key,
debit_credit,
amount_local,
amount_tax,
item_text,
gl_account,
baseline_date,
payment_terms,
payment_block,
follow_on_doc,
dunning_block,
dunning_key,
dunning_date_last,
dunning_level,
dunning_area,
billing_document,
credit_control_area,
days1,
days2,
days3,
reference_key1,
file_path,
transaction_key,
download_date,
src_download_date,
document_posted_by,
accounting_clerk,
reconciliation_account,
customer_country,
customer_name1,
customer_name2,
trading_partner,
accounting_clerk_name,
accounting_clerk_user,
customer_name_chinese,
credit_limit,
credit_account,
risk_category,
credit_block,
last_internal_review,
credit_reporting_group,
dispute_object_type,
dispute_coordinator,
dispute_process_deadline,
dispute_detailed_cause,
dispute_case_id,
dispute_case_type,
dispute_case_title,
dispute_planned_close_date,
dispute_reason,
dispute_status,
dispute_responsible,
dispute_processor,
dispute_created_on,
dispute_closed_on,
dispute_changed_on,
key_date,
due_date,
arrears_after_net,
amount_eur,
due_date_vat,
arrears_after_net_vat,
overdue_rank,
overdue_value,
relevant_for_payment_behavior, 
amount_document, 
posting_to_clearing_days, 
vat_issued,
reason, 
reason_details, 
days1_vat,
days2_vat, 
reverse_document,
overdue_rank_vat,
sample_order
into o2c.sta_open_cust_items
from o2c.tp3_all_cust_items
where posting_date <= @keydate_tp3 and 
	( clearing_date is null or clearing_date > @keydate_tp3 );


while (@month > -11)
begin 

set @keydate_tp3 = EOMONTH(DATEADD(MONTH,-1,@keydate_tp3))

insert into o2c.sta_open_cust_items (
company_code,
document_number,
fiscal_year,
line_item,
customer_number,
special_gl_indicator,
clearing_date,
clearing_document,
assignment_number,
posting_date,
document_date,
entry_date,
currency,
reference,
document_type,
posting_key,
debit_credit,
amount_local,
amount_tax,
item_text,
gl_account,
baseline_date,
payment_terms,
payment_block,
follow_on_doc,
dunning_block,
dunning_key,
dunning_date_last,
dunning_level,
dunning_area,
billing_document,
credit_control_area,
days1,
days2,
days3,
reference_key1,
file_path,
transaction_key,
download_date,
src_download_date,
document_posted_by,
accounting_clerk,
reconciliation_account,
customer_country,
customer_name1,
customer_name2,
trading_partner,
accounting_clerk_name,
accounting_clerk_user,
customer_name_chinese,
credit_limit,
credit_account,
risk_category,
credit_block,
last_internal_review,
credit_reporting_group,
dispute_object_type,
dispute_coordinator,
dispute_process_deadline,
dispute_detailed_cause,
dispute_case_id,
dispute_case_type,
dispute_case_title,
dispute_planned_close_date,
dispute_reason,
dispute_status,
dispute_responsible,
dispute_processor,
dispute_created_on,
dispute_closed_on,
dispute_changed_on,
key_date,
due_date,
arrears_after_net,
amount_eur,
due_date_vat,
arrears_after_net_vat,
overdue_rank,
overdue_value,
relevant_for_payment_behavior,
amount_document,
posting_to_clearing_days,
vat_issued,
reason, 
reason_details, 
days1_vat,
days2_vat, 
reverse_document, 
overdue_rank_vat,
sample_order)
select 
company_code,
document_number,
fiscal_year,
line_item,
customer_number,
special_gl_indicator,
clearing_date,
clearing_document,
assignment_number,
posting_date,
document_date,
entry_date,
currency,
reference,
document_type,
posting_key,
debit_credit,
amount_local,
amount_tax,
item_text,
gl_account,
baseline_date,
payment_terms,
payment_block,
follow_on_doc,
dunning_block,
dunning_key,
dunning_date_last,
dunning_level,
dunning_area,
billing_document,
credit_control_area,
days1,
days2,
days3,
reference_key1,
file_path,
transaction_key,
download_date,
src_download_date,
document_posted_by,
accounting_clerk,
reconciliation_account,
customer_country,
customer_name1,
customer_name2,
trading_partner,
accounting_clerk_name,
accounting_clerk_user,
customer_name_chinese,
credit_limit,
credit_account,
risk_category,
credit_block,
last_internal_review,
credit_reporting_group,
dispute_object_type,
dispute_coordinator,
dispute_process_deadline,
dispute_detailed_cause,
dispute_case_id,
dispute_case_type,
dispute_case_title,
dispute_planned_close_date,
dispute_reason,
dispute_status,
dispute_responsible,
dispute_processor,
dispute_created_on,
dispute_closed_on,
dispute_changed_on,
null,
due_date,
arrears_after_net,
amount_eur,
due_date_vat,
arrears_after_net_vat,
overdue_rank,
overdue_value,
relevant_for_payment_behavior, 
amount_document, 
posting_to_clearing_days, 
vat_issued,
reason, 
reason_details, 
days1_vat,
days2_vat, 
reverse_document,
overdue_rank_vat, 
sample_order
from o2c.tp3_all_cust_items
where posting_date <= @keydate_tp3 and 
	( clearing_date is null or clearing_date > @keydate_tp3 );

update o2c.sta_open_cust_items set key_date = @keydate_tp3 
where key_date is null

set @month = @month -1 
end

update o2c.sta_open_cust_items set arrears_after_net = dbo.fc_calculate_arrears(due_date,key_date) 

update o2c.sta_open_cust_items set arrears_after_net_vat = dbo.fc_calculate_arrears(due_date_vat,key_date) 
where payment_terms <> '' and
	  reference_key1 <> ''

update o2c.sta_open_cust_items set arrears_after_net_vat = arrears_after_net
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.sta_open_cust_items set overdue_rank_vat = ''

update o2c.sta_open_cust_items set overdue_rank_vat = '1-30'
where arrears_after_net_vat > 0 and arrears_after_net_vat <= 30

update o2c.sta_open_cust_items set overdue_rank_vat = '31-90'
where arrears_after_net_vat > 30 and arrears_after_net_vat <= 90

update o2c.sta_open_cust_items set overdue_rank_vat = '90+'
where arrears_after_net_vat > 90

update o2c.sta_open_cust_items set overdue_rank_vat = 'not_due'
where arrears_after_net_vat <=0

update o2c.sta_open_cust_items set overdue_rank = ''

update o2c.sta_open_cust_items set overdue_rank = '1-30'
where arrears_after_net > 0 and arrears_after_net <= 30

update o2c.sta_open_cust_items set overdue_rank = '31-90'
where arrears_after_net > 30 and arrears_after_net <= 90

update o2c.sta_open_cust_items set overdue_rank = '90+'
where arrears_after_net > 90

update o2c.sta_open_cust_items set overdue_rank = 'not_due'
where arrears_after_net <=0

exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_open_cust_items', @schema = 'o2c'
GO
/****** Object:  StoredProcedure [o2c].[p_sta_payment_behavior]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_payment_behavior] as 
begin

drop table if exists o2c.sta_payment_behavior
drop table if exists #temp_od
drop table if exists #temp_sum 
drop table if exists #temp_cust;

with sum_od as (
    select distinct credit_account,
		   key_date,
		   overdue_rank_vat,
		   sum(convert(float,overdue_value) ) over (
            partition by 
				convert(varchar(10),credit_account), 
				key_date,
				convert(varchar(10),overdue_rank_vat) 
            order by 
                convert(varchar(10),credit_account),
				key_date ,
				convert(varchar(10),overdue_rank)  
			) overdue_value_by_ca 
     from 
        o2c.sta_open_cust_items
	 where credit_account is not null 
	 and relevant_for_payment_behavior = 'x')


select * into #temp_od from (
  select credit_account, key_date, overdue_rank_vat,overdue_value_by_ca
  from sum_od
) t
pivot (
  sum(overdue_value_by_ca)
  for overdue_rank_vat in (
   [1-30], [90+], [31-90], [not_due]
  ) 
)as p;


with sum_sales as (
    select distinct credit_account,
		   sum(convert(float,amount_local) ) over (
            partition by 
				convert(varchar(10),credit_account)
            order by 
                convert(varchar(10),credit_account)
			) sales_by_ca
     from 
        o2c.sta_all_cust_items
	 where (document_type = 'dg' or 
		   document_type = 'dr') and 
		   posting_date between eomonth(dateadd(month,-12,key_date)) and key_date)

select * into #temp_sum 
from sum_sales

select * into #temp_cust 
from o2c.tp1_customer 
where customer_number = credit_account;

with del_cust_duplicates as (
    select 
        *,
        row_number() over (
            partition by 
				credit_account
            order by 
				credit_account,
				company_code 
        ) row_num
     from 
        #temp_cust
)

delete from del_cust_duplicates
where row_num > 1

select od.credit_account, 
	   od.key_date, 
	   od.[1-30], 
	   od.[31-90], 
	   od.[90+],
	   od.[not_due],
	   su.sales_by_ca, 
	   cust.credit_limit,
	   cust.credit_reporting_group,
	   cust.customer_country,
	   cust.customer_name_chinese,
	   cust.last_internal_review,
	   cust.reconciliation_account,
	   cust.risk_category,
	   cust.trading_partner,
	   cust.credit_block
into o2c.sta_payment_behavior
from #temp_od as od 
	left join 
	#temp_sum as su on 
	od.credit_account = su.credit_account 
	left join 
	#temp_cust as cust on 
	right('0000000000' + convert(varchar(10),od.credit_account), 10) = cust.credit_account

drop table if exists #temp_od
drop table if exists #temp_sum 
drop table if exists #temp_cust

end
GO
/****** Object:  StoredProcedure [o2c].[p_tp1_exec]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp1_exec] as

exec o2c.p_tp10_customer

exec o2c.p_tp10_all_cust_items

exec o2c.p_tp10_dispute
GO
/****** Object:  StoredProcedure [o2c].[p_tp10_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp10_all_cust_items] as

drop table if exists o2c.tp1_all_cust_items

select 
	bsid.*, 
	bkpf.document_posted_by
	into o2c.tp1_all_cust_items 
	from o2c.cln_bsid as bsid
	left outer join o2c.cln_bkpf as bkpf on 
	bkpf.document_number = bsid.document_number and 
	bkpf.company_code = bsid.company_code and 
	bkpf.fiscal_year = bsid.fiscal_year
union
	select bsad.*, 
	bkpf.document_posted_by
	from o2c.cln_bsad as bsad
	left outer join o2c.cln_bkpf as bkpf on 
	bkpf.document_number = bsad.document_number and 
	bkpf.company_code = bsad.company_code and 
	bkpf.fiscal_year = bsad.fiscal_year;


-- because BSID and BSAD tables are downloaded at slightly different times there can be duplicates e.g. one item was open while bsid
-- was downlaoded an then cleared before bsad was downlaoded

with duplicates as (
    select *,
        row_number() over (
            partition by 
                transaction_key
            order by 
				transaction_key, 
				src_download_date desc
        ) row_num
     from 
		 o2c.tp1_all_cust_items
)

delete from duplicates
where row_num > 1
GO
/****** Object:  StoredProcedure [o2c].[p_tp10_customer]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp10_customer] as

drop table if exists o2c.tp1_customer

select cust.*, 
	   cust_m.customer_country, 
	   cust_m.customer_name1,
	   cust_m.customer_name2, 
	   cust_m.trading_partner, 
	   busab.accounting_clerk_name, 
	   busab.accounting_clerk_user, 
	   adrc.name1 as customer_name_chinese,
	   knkk.credit_control_area, 
	   knkk.credit_limit, 
	   knkk.credit_account, 
	   knkk.risk_category, 
	   knkk.block_indicator as credit_block,
	   knkk.last_internal_review, 
	   knkk.credit_reporting_group 
into o2c.tp1_customer
from o2c.cln_knb1 as cust 
	 left outer join  
	 o2c.cln_kna1 as cust_m on 
	 cust.customer_number = cust_m.customer_number
	 left outer join 
	 o2c.cln_t001s as busab on 
	 cust.accounting_clerk = busab.accounting_clerk_number and 
	 cust.company_code = busab.company_code
	 left outer join
	 o2c.cln_adrc as adrc on 
	 cust_m.address_number = adrc.address_number 
	 left outer join 
	 o2c.cln_knkk as knkk on 
	 cust.customer_number = knkk.customer_number
	 and credit_control_area = '0083'
	 left outer join 
	 o2c.cln_t001 as bukrs on 
	 cust.company_code = bukrs.company_code

update o2c.tp1_customer set customer_name_chinese = concat(customer_name1,' ', customer_name2)
where customer_name_chinese is null
GO
/****** Object:  StoredProcedure [o2c].[p_tp10_dispute]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp10_dispute] as

drop table if exists o2c.tp1_dispute

select dc.*, 
udm.dispute_coordinator,
udm.dispute_process_deadline,
udm.dispute_detailed_cause,
scmg.dispute_case_id,
scmg.dispute_case_type,
scmg.dispute_case_title,
scmg.dispute_planned_close_date,
scmg.dispute_reason,
scmg.dispute_status,
scmg.dispute_responsible,
scmg.dispute_processor,
scmg.dispute_created_on,
scmg.dispute_closed_on,
scmg.dispute_changed_on
into o2c.tp1_dispute 
from o2c.cln_fdm_dcproc as dc 
	 left outer join 
	 o2c.cln_udmcaseattr00 as udm on
	 dc.dispute_id = udm.dispute_id 
	 left outer join 
	 o2c.cln_scmg_t_case_attr as scmg on 
	 dc.dispute_id = scmg.dispute_id
GO
/****** Object:  StoredProcedure [o2c].[p_tp2_exec]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp2_exec] as 

exec o2c.p_tp20_all_cust_items

exec o2c.p_tp20_fi1000
GO
/****** Object:  StoredProcedure [o2c].[p_tp20_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp20_all_cust_items] as 

drop table if exists o2c.tp2_all_cust_items

select items.*, 
cust.accounting_clerk,
cust.reconciliation_account,
cust.customer_country,
cust.customer_name1,
cust.customer_name2,
cust.trading_partner,
cust.accounting_clerk_name,
cust.accounting_clerk_user,
cust.customer_name_chinese,
cust.credit_limit,
cust.credit_account,
cust.risk_category,
cust.credit_block,
cust.last_internal_review,
cust.credit_reporting_group,
disp.dispute_object_type,
disp.dispute_coordinator,
disp.dispute_process_deadline,
disp.dispute_detailed_cause,
disp.dispute_case_id,
disp.dispute_case_type,
disp.dispute_case_title,
disp.dispute_planned_close_date,
disp.dispute_reason,
disp.dispute_status,
disp.dispute_responsible,
disp.dispute_processor,
disp.dispute_created_on,
disp.dispute_closed_on,
disp.dispute_changed_on, 
sam.sample_order,
reason.reason,
reason.reason_details
into o2c.tp2_all_cust_items 
from o2c.tp1_all_cust_items as items 
	 left outer join o2c.tp1_customer as cust on
	 items.customer_number = cust.customer_number and 
	 items.company_code = cust.company_code 
	 left outer join o2c.tp1_dispute as disp on 
	 items.transaction_key = disp.transaction_key
	 left outer join o2c.cln_sample_orders as sam on
	 items.company_code = sam.company_code and 
	 items.document_number = sam.document_number and 
	 items.line_item = sam.line_item and 
	 items.fiscal_year = sam.[year] 
	 left outer join dbo.cln_overdue_reason as reason on 
	 items.company_code = reason.company_code and 
	 items.document_number = reason.document_number and 
	 items.line_item = reason.line_item and 
	 items.fiscal_year = reason.[year] 
GO
/****** Object:  StoredProcedure [o2c].[p_tp20_fi1000]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_tp20_fi1000]
AS
BEGIN
--Statisit table eflowclr
	drop  table if exists o2c.tp2_fi1000;

	select 
	b.*, 
	a.business_division,
	a.business_unit,
	a.division,
	a.postdate,
	a.salesamount,
    a.aramount,
	a.overdueamount
	into o2c.tp2_fi1000 from   
	(
	select  substring(company_code,9,len(company_code)-8) as company_code_a, 
	right(concat('0000000000',substring(customer_number,9,len(customer_number)-8)),10) as customer_number_a, 
	business_division,business_unit,
	substring(credit_control_area,9,len(credit_control_area)-8) as credit_control_area_a,
	division,postdate,aramount,salesamount,overdueamount
	from o2c.cln_fi1000 
	) a
	left join 
	o2c.tp1_customer b
	on a.company_code_a  = b.company_code
	and a.customer_number_a = b.customer_number

END
GO
/****** Object:  StoredProcedure [o2c].[p_tp3_exec]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp3_exec] as 

exec o2c.p_tp30_first

exec o2c.p_tp30_all_cust_items 
GO
/****** Object:  StoredProcedure [o2c].[p_tp30_all_cust_items]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp30_all_cust_items] as 

update o2c.tp3_all_cust_items set key_date = eomonth(src_download_date,-1)

update o2c.tp3_all_cust_items set due_date = o2c.fc_calculate_due_date_ar(convert(date, baseline_date,104),convert(int,days1),convert(int,days2), debit_credit,follow_on_doc) 

update o2c.tp3_all_cust_items set arrears_after_net = dbo.fc_calculate_arrears(due_date,key_date)
where clearing_date is null 

update o2c.tp3_all_cust_items set arrears_after_net = dbo.fc_calculate_arrears(due_date,clearing_date)
where clearing_date is not null 

update o2c.tp3_all_cust_items set amount_local = amount_local * -1 where debit_credit = 'H'

update o2c.tp3_all_cust_items set amount_tax = amount_tax * -1 where debit_credit = 'H'

update o2c.tp3_all_cust_items set amount_document  = amount_document * -1 where debit_credit = 'H'

update o2c.tp3_all_cust_items set company_code_currency = 'HKD' where company_code = '0078'

update o2c.tp3_all_cust_items set company_code_currency = 'CNY' where company_code <> '0078'

update o2c.tp3_all_cust_items set amount_eur = o2c.fc_convert_currency(company_code_currency,convert(decimal(30,2),amount_local), 'EUR') 

update o2c.tp3_all_cust_items set dispute_created_on = LEFT(dispute_created_on,8)

update o2c.tp3_all_cust_items set dispute_changed_on = LEFT(dispute_changed_on,8)

update o2c.tp3_all_cust_items set dispute_closed_on = LEFT(dispute_closed_on,8)

update o2c.tp3_all_cust_items set dispute_closed_on = null 
where left(dispute_closed_on,1) <> '2' and 
	  left(dispute_closed_on,1) <> ''

update o2c.tp3_all_cust_items set reference_key1 = '' 
where left(reference_key1,1) <> '2' and 
	  left(reference_key1,1) <> ''

update o2c.tp3_all_cust_items set days1_vat = o2c.fc_calculate_days1(payment_terms, reference_key1)
where payment_terms <> '' and  
	  reference_key1 <> '' 

update o2c.tp3_all_cust_items set days1_vat = days1
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set days2_vat = o2c.fc_calculate_days2(payment_terms, reference_key1)
where payment_terms <> '' and
	  reference_key1 <> ''

update o2c.tp3_all_cust_items set days2_vat = days2
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set due_date_vat = o2c.fc_calculate_due_date_ar(convert(date, reference_key1,112),days1_vat,days2_vat, debit_credit,follow_on_doc)
where payment_terms <> '' and
	  reference_key1 <> ''

update o2c.tp3_all_cust_items set due_date_vat = due_date
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set arrears_after_net_vat = dbo.fc_calculate_arrears(due_date_vat,key_date) 
where payment_terms <> '' and
	  reference_key1 <> '' and 
	  clearing_date is null 

update o2c.tp3_all_cust_items set arrears_after_net_vat = dbo.fc_calculate_arrears(due_date_vat,clearing_date) 
where payment_terms <> '' and
	  reference_key1 <> '' and 
	  clearing_date is not null

update o2c.tp3_all_cust_items set arrears_after_net_vat = arrears_after_net
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set posting_to_clearing_days = dbo.FC_GET_BUSINESS_DAYS(CONVERT(date, posting_date,104), CONVERT(date, clearing_date,104)) 

update o2c.tp3_all_cust_items set relevant_for_payment_behavior = 'X' 
where 
	(company_code = '0078' 
	 and debit_credit = 'S') 
	 or 
	(company_code = '0083'  
	 and debit_credit = 'S'
	 and not contains(item_text, 'quality')
	 and not contains(item_text, 'price')
	 and not contains(item_text, 'write')
	 and not contains(item_text, 'sample'))
	 or 
	 (company_code = '0289'
	 and debit_credit = 'S') 
	 or 
	 (company_code = '0369'
	 and debit_credit = 'S') 
	 or 
	 (company_code = '0199' 
	 and debit_credit = 'S' 
	 and reference <> LEFT('INV.',4)
	 and not contains(item_text, 'price')
	 and not contains(item_text, 'deduction') 
	 and not contains(item_text, '保证金') 
	 and not contains(item_text, '质量')
	 and not contains(item_text, '质保金')
	 and not contains(item_text, '三包')
	 and not contains(item_text, '扣款')
	 and not contains(item_text, '折扣')
	 and not contains(item_text, '折让')
	 and not contains(item_text, '税'))

update o2c.tp3_all_cust_items set relevant_for_payment_behavior = '' 
where reverse_document = 'X' and MONTH(key_date) < MONTH(clearing_date)

update o2c.tp3_all_cust_items set overdue_rank = '1-30'
where arrears_after_net > 0 and arrears_after_net <= 30

update o2c.tp3_all_cust_items set overdue_rank = '31-90'
where arrears_after_net > 30 and arrears_after_net <= 90

update o2c.tp3_all_cust_items set overdue_rank = '90+'
where arrears_after_net > 90

update o2c.tp3_all_cust_items set overdue_rank = 'not_due'
where arrears_after_net <= 0

update o2c.tp3_all_cust_items set overdue_rank_vat = '1-30'
where arrears_after_net_vat > 0 and arrears_after_net_vat <= 30

update o2c.tp3_all_cust_items set overdue_rank_vat = '31-90'
where arrears_after_net_vat > 30 and arrears_after_net_vat <= 90

update o2c.tp3_all_cust_items set overdue_rank_vat = '90+'
where arrears_after_net_vat > 90

update o2c.tp3_all_cust_items set overdue_rank_vat = 'not_due'
where arrears_after_net_vat <= 0

update o2c.tp3_all_cust_items set overdue_value = amount_local
where relevant_for_payment_behavior = 'X' 

update o2c.tp3_all_cust_items set vat_issued = 'VAT issued' where left(REFERENCE,3) = 'INV'
update o2c.tp3_all_cust_items set vat_issued = 'no VAT' where left(REFERENCE,3) <> 'INV'
GO
/****** Object:  StoredProcedure [o2c].[p_tp30_first]    Script Date: 4/8/2021 1:59:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp30_first] as

drop table if exists o2c.tp3_fi1000

select * into o2c.tp3_fi1000
from o2c.tp2_fi1000

if col_length ('o2c.tp3_fi1000','payment_behavior_ar_amount') is null
begin
	alter table o2c.tp3_fi1000
	add payment_behavior_ar_amount float
end

if col_length ('o2c.tp3_fi1000','sum_overdue_0_30') is null
begin
	alter table o2c.tp3_fi1000
	add sum_overdue_0_30 float
end

if col_length ('o2c.tp3_fi1000','sum_overdue_30_60') is null
begin
	alter table o2c.tp3_fi1000
	add sum_overdue_30_60 float
end

if col_length ('o2c.tp3_fi1000','sum_overdue_60') is null
begin
	alter table o2c.tp3_fi1000
	add sum_overdue_60 float
end

if col_length ('o2c.tp3_fi1000','receiveables_0_30') is null
begin
	alter table o2c.tp3_fi1000
	add receiveables_0_30 varchar(max)
end

if col_length ('o2c.tp3_fi1000','receiveables_30_60') is null
begin
	alter table o2c.tp3_fi1000
	add receiveables_30_60 varchar(max)
end

if col_length ('o2c.tp3_fi1000','receiveables_60') is null
begin
	alter table o2c.tp3_fi1000
	add receiveables_60 varchar(max)
end

drop table if exists o2c.tp3_all_cust_items 

create table [o2c].[tp3_all_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[file_path] [nvarchar](max) NULL,
	transaction_key nvarchar(50) not null,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[reverse_document][nvarchar](max) NULL,
	[sample_order][nvarchar](max) NULL
		primary key (transaction_key)
) on [primary] textimage_on [primary]

create unique index i1 on o2c.tp3_all_cust_items(transaction_key);

create fulltext index on  o2c.tp3_all_cust_items (
    item_text language 0
) key index i1
with 
    change_tracking = auto, 
    stoplist=off
;

insert into [o2c].[tp3_all_cust_items]
           ([company_code]
           ,[document_number]
           ,[fiscal_year]
           ,[line_item]
           ,[customer_number]
           ,[special_gl_indicator]
           ,[clearing_date]
           ,[clearing_document]
           ,[assignment_number]
           ,[posting_date]
           ,[document_date]
           ,[entry_date]
           ,[currency]
           ,[reference]
           ,[document_type]
           ,[posting_key]
           ,[debit_credit]
           ,[amount_local]
           ,[amount_tax]
           ,[item_text]
           ,[gl_account]
           ,[baseline_date]
           ,[payment_terms]
           ,[payment_block]
           ,[follow_on_doc]
           ,[dunning_block]
           ,[dunning_key]
           ,[dunning_date_last]
           ,[dunning_level]
           ,[dunning_area]
           ,[billing_document]
           ,[credit_control_area]
           ,[days1]
           ,[days2]
           ,[days3]
           ,[reference_key1]
           ,[amount_document]
           ,[file_path]
           ,[transaction_key]
           ,[download_date]
           ,[src_download_date]
           ,[document_posted_by]
           ,[accounting_clerk]
           ,[reconciliation_account]
           ,[customer_country]
           ,[customer_name1]
           ,[customer_name2]
           ,[trading_partner]
           ,[accounting_clerk_name]
           ,[accounting_clerk_user]
           ,[customer_name_chinese]
           ,[credit_limit]
           ,[credit_account]
           ,[risk_category]
           ,[credit_block]
           ,[last_internal_review]
           ,[credit_reporting_group]
           ,[dispute_object_type]
           ,[dispute_coordinator]
           ,[dispute_process_deadline]
           ,[dispute_detailed_cause]
           ,[dispute_case_id]
           ,[dispute_case_type]
           ,[dispute_case_title]
           ,[dispute_planned_close_date]
           ,[dispute_reason]
           ,[dispute_status]
           ,[dispute_responsible]
           ,[dispute_processor]
           ,[dispute_created_on]
           ,[dispute_closed_on]
           ,[dispute_changed_on]
		   ,[reason]
		   ,[reason_details]
		   ,[reverse_document]
		   ,[sample_order])
select 
            [company_code]
           ,[document_number]
           ,[fiscal_year]
           ,[line_item]
           ,[customer_number]
           ,[special_gl_indicator]
           ,[clearing_date]
           ,[clearing_document]
           ,[assignment_number]
           ,[posting_date]
           ,[document_date]
           ,[entry_date]
           ,[currency]
           ,[reference]
           ,[document_type]
           ,[posting_key]
           ,[debit_credit]
           ,[amount_local]
           ,[amount_tax]
           ,[item_text]
           ,[gl_account]
           ,[baseline_date]
           ,[payment_terms]
           ,[payment_block]
           ,[follow_on_doc]
           ,[dunning_block]
           ,[dunning_key]
           ,[dunning_date_last]
           ,[dunning_level]
           ,[dunning_area]
           ,[billing_document]
           ,[credit_control_area]
           ,[days1]
           ,[days2]
           ,[days3]
           ,[reference_key1]
           ,[amount_document]
           ,[file_path]
           ,[transaction_key]
           ,[download_date]
           ,[src_download_date]
           ,[document_posted_by]
           ,[accounting_clerk]
           ,[reconciliation_account]
           ,[customer_country]
           ,[customer_name1]
           ,[customer_name2]
           ,[trading_partner]
           ,[accounting_clerk_name]
           ,[accounting_clerk_user]
           ,[customer_name_chinese]
           ,[credit_limit]
           ,[credit_account]
           ,[risk_category]
           ,[credit_block]
           ,[last_internal_review]
           ,[credit_reporting_group]
           ,[dispute_object_type]
           ,[dispute_coordinator]
           ,[dispute_process_deadline]
           ,[dispute_detailed_cause]
           ,[dispute_case_id]
           ,[dispute_case_type]
           ,[dispute_case_title]
           ,[dispute_planned_close_date]
           ,[dispute_reason]
           ,[dispute_status]
           ,[dispute_responsible]
           ,[dispute_processor]
           ,[dispute_created_on]
           ,[dispute_closed_on]
           ,[dispute_changed_on]
		   ,[reason]
		   ,[reason_details]
		   ,[reverse_document]
		   ,[sample_order]
from o2c.tp2_all_cust_items

if col_length ('o2c.tp3_all_cust_items','key_date') is null
begin
	alter table o2c.tp3_all_cust_items
	add key_date date
end

if col_length ('o2c.tp3_all_cust_items','company_code_currency') is null
begin
	alter table o2c.tp3_all_cust_items
	add company_code_currency varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','due_date') is null 
begin
	alter table o2c.tp3_all_cust_items
	add due_date varchar(max)
end 

if col_length ('o2c.tp3_all_cust_items','arrears_after_net') is null 
begin
	alter table o2c.tp3_all_cust_items
	add arrears_after_net int
	create index i2 on o2c.tp3_all_cust_items(arrears_after_net);
end

if col_length ('o2c.tp3_all_cust_items','amount_eur') is null
begin
	alter table o2c.tp3_all_cust_items
	add amount_eur varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','amount_eur') is null
begin
	alter table o2c.tp3_all_cust_items
	add amount_eur varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','due_date_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add due_date_vat date
end

if col_length ('o2c.tp3_all_cust_items','arrears_after_net_vat') is null 
begin
	alter table o2c.tp3_all_cust_items
	add arrears_after_net_vat int 
	create index i3 on o2c.tp3_all_cust_items(arrears_after_net_vat);
end

if col_length ('o2c.tp3_all_cust_items','overdue_rank') is null 
begin
	alter table o2c.tp3_all_cust_items
	add overdue_rank varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','overdue_value') is null 
begin
	alter table o2c.tp3_all_cust_items
	add overdue_value varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','relevant_for_payment_behavior') is null 
begin
	alter table o2c.tp3_all_cust_items
	add relevant_for_payment_behavior varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','days1_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add days1_vat int
end

if col_length ('o2c.tp3_all_cust_items','days2_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add days2_vat int
end

if col_length ('o2c.tp3_all_cust_items','posting_to_clearing_days') is null
begin
	alter table o2c.tp3_all_cust_items
	add posting_to_clearing_days int
end

if col_length ('o2c.tp3_all_cust_items','vat_issued') is null
begin
	alter table o2c.tp3_all_cust_items
	add vat_issued varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','overdue_rank_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add overdue_rank_vat varchar(max)
end

GO
EXEC sys.sp_addextendedproperty @name=N'microsoft_database_tools_support', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'sysdiagrams'
GO
ALTER DATABASE [sdp-s-fssc] SET  READ_WRITE 
GO
