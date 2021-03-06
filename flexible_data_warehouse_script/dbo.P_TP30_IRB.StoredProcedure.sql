/****** Object:  StoredProcedure [dbo].[P_TP30_IRB]    Script Date: 4/16/2021 11:35:28 AM ******/
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
