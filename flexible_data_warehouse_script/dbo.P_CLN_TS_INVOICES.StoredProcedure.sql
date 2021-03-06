/****** Object:  StoredProcedure [dbo].[P_CLN_TS_INVOICES]    Script Date: 4/16/2021 11:35:28 AM ******/
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
