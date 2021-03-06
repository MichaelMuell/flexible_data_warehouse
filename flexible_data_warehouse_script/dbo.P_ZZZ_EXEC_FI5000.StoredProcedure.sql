/****** Object:  StoredProcedure [dbo].[P_ZZZ_EXEC_FI5000]    Script Date: 4/16/2021 11:35:28 AM ******/
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
