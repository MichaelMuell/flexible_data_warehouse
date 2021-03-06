/****** Object:  StoredProcedure [dbo].[update_metadata]    Script Date: 4/16/2021 11:35:28 AM ******/
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
