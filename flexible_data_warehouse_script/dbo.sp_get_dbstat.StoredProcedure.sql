/****** Object:  StoredProcedure [dbo].[sp_get_dbstat]    Script Date: 4/16/2021 11:35:28 AM ******/
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
