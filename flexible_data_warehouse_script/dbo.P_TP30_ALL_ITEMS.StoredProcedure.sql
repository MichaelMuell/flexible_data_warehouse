/****** Object:  StoredProcedure [dbo].[P_TP30_ALL_ITEMS]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP30_ALL_ITEMS] AS

UPDATE TP3_ALL_ITEMS SET KEY_DATE = EOMONTH(SRC_DOWNLOAD_DATE,-1)

UPDATE TP3_ALL_ITEMS SET DUE_DATE = DBO.FC_CALCULATE_DUE_DATE_AP(CONVERT(DATE, BASELINE_DATE,104),CONVERT(INT,DAYS1),CONVERT(INT,DAYS2), DEBIT_CREDIT,FOLLOW_ON_DOC) 

UPDATE TP3_ALL_ITEMS SET ARREARS_AFTER_NET = DBO.FC_CALCULATE_ARREARS(DUE_DATE,KEY_DATE) 

UPDATE TP3_ALL_ITEMS SET TRANSACTION_KEY = CONCAT([YEAR],COMPANY_CODE,DOCUMENT_NUMBER,LINE_ITEM)

UPDATE TP3_ALL_ITEMS SET AMOUNT_LOCAL = AMOUNT_LOCAL * -1 WHERE DEBIT_CREDIT = 'H'

UPDATE TP3_ALL_ITEMS SET AMOUNT_DOCUMENT = AMOUNT_DOCUMENT * -1 WHERE DEBIT_CREDIT = 'H'

UPDATE TP3_ALL_ITEMS SET WHT = 'WITHHOLDING TAX' WHERE LEFT(REFERENCE,3) = 'WHT' OR RIGHT(REFERENCE,3) = 'EIT'  

UPDATE TP3_ALL_ITEMS SET DUPLICATE = 'X' WHERE CHARINDEX('V',REFERENCE) <> 0

GO
