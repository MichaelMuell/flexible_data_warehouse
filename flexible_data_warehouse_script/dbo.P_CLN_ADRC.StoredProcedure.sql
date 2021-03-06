/****** Object:  StoredProcedure [dbo].[P_CLN_ADRC]    Script Date: 4/16/2021 11:35:28 AM ******/
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
