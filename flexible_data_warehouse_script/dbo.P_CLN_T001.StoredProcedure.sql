/****** Object:  StoredProcedure [dbo].[P_CLN_T001]    Script Date: 4/16/2021 11:35:28 AM ******/
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
