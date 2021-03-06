/****** Object:  StoredProcedure [dbo].[P_CLN_OCRLOG]    Script Date: 4/16/2021 11:35:28 AM ******/
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
