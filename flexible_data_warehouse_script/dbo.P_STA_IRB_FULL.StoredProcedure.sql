/****** Object:  StoredProcedure [dbo].[P_STA_IRB_FULL]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_STA_IRB_FULL] AS 

-- Standard Model of all items --> Weekly Update --> Always create from Scratch
DROP TABLE IF EXISTS STA_IRB_FULL

DECLARE @max_posting_date as date 

SET @max_posting_date = (Select EOMONTH(MAX(CONVERT(date,POSTING_DATE,104))) from TP3_IRB)

SELECT 
            [ACTIVITY_STATUS]
           ,[YEAR]
           ,[CASH_DISCOUNT2]
           ,[CLEARING_DATE]
           ,[COMPANY_CODE]
           ,[CURRENCY]
           ,[ENTERED_ON_DATE]
           ,[FI_DOCUMENT_NO]
           ,[INPUT_DATE]
           ,[INVOICE_DATE]
           ,[INVOICE_INPUT_CHANNEL]
           ,[INVOICE_STATE]
           ,[LEGAL_ENTITY]
           ,[OCR_INVOICE_CORRECTION]
           ,[OCR_STACK_NAME]
           ,[OCR_SUPPLIER_CORRECTION]
           ,[PAYING_DATE]
           ,[POSTING_DATE]
           ,[PURCHASE_ORDER]
           ,[REFERENCE]
           ,[SCANTIME]
           ,[SOURCE_SYSTEM]
           ,[STATE_AUTO_POSTING]
           ,[VENDOR_NUMBER]
           ,[TEAM_HIST]
           ,[TRANSACTION_KEY]
           ,[AMOUNT_DOCUMENT]
           ,[AMOUNT_LOCAL]
           ,[TAX_AMOUNT]
           ,[AUTH_LEGAL_REGION]
           ,[ACTIVITY_STATUS_DESCRIPTION]
           ,[INVOICE_STATE_DESCRIPTION]
           ,[CURRENCY_DESCRIPTION]
           ,[SUPPLIER_CORRECTION_DESCRIPTION]
           ,[STATE_AUTO_POSTING_DESCRIPTION]
           ,[INVOICE_CORRECTION_DESCRIPTION]
           ,[SCAN_DATE]
           ,[DELIVERY_NOTE]
           ,[MAT_DOCUMENT_NO]
           ,[SRC_DOWNLOAD_DATE]
           ,[TS_FILENAME]
           ,[TS_FAPIAO_CODE]
           ,[PURCHASING_GROUP]
           ,[PO_CREATED_BY]
           ,[PO_COMPANY_CODE]
           ,[REQUISITIONER]
           ,[PR_CREATOR]
           ,[COMPANY_NAME]
           ,[COMPANY_NAME_SHORT]
           ,[PURCHASER_NAME]
           ,[GR_QUANTITY]
           ,[IR_QUANTITY]
           ,[TS_ERROR_01]
           ,[TS_ERROR_02]
           ,[TS_ERROR_03]
           ,[TS_ERROR_04]
           ,[TS_ERROR_05]
           ,[TS_ERROR_06]
           ,[TS_ERROR_07]
           ,[TS_ERROR_08]
           ,[TS_PO_REMARK]
           ,[ACCOUNTING_CLERK_NAME]
           ,[ACCOUNTING_CLERK_NUMBER]
           ,[ACCOUNTING_CLERK_USER]
           ,[RECONCILIATION_ACCOUNT]
           ,[TRADING_PARTNER]
           ,[VENDOR_COUNTRY]
           ,[VENDOR_NAME]
           ,[VENDOR_NAME_CHINESE]
           ,[REFERENCE_DOCUMENT]
           ,[YEAR_REF_DOC]
           ,[REF_DOC_ENTRY_DATE]
           ,[REF_DOC_POSTING_DATE]
           ,[REF_DOC_CREATED_BY]
           ,[SCAN_DATE_TO_INPUT_DATE]
           ,[INPUT_DATE_TO_POSTING_DATE]
           ,[EIV_AUTOPOST]
           ,[KEY_DATE]
     INTO dbo.STA_IRB_FULL 
	 FROM TP3_IRB

DELETE FROM STA_IRB_FULL WHERE 
CONVERT(date,POSTING_DATE,104) <= DATEADD(MONTH,-2,DATEADD(YEAR,-1,@max_posting_date))

EXEC o2c.P_EXECUTE_ETL_FUNCTION @Imp_Function = 'REMOVE_ZERO', @Imp_TableName = 'STA_IRB_FULL', @schema = 'dbo'
GO
