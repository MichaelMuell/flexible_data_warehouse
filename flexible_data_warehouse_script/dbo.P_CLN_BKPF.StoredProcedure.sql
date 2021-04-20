/****** Object:  StoredProcedure [dbo].[P_CLN_BKPF]    Script Date: 4/16/2021 11:35:28 AM ******/
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
WHERE TABLE_NAME = 'BKPF_FSSC_Improvement_Framework'
GO
