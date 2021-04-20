/****** Object:  StoredProcedure [dbo].[P_CLN_EBAN]    Script Date: 4/16/2021 11:35:28 AM ******/
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
