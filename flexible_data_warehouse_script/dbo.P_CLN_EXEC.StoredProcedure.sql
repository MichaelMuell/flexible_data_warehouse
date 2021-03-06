/****** Object:  StoredProcedure [dbo].[P_CLN_EXEC]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_CLN_EXEC] AS

-- FIRST---------------------------------------------------------------------

EXEC P_CLN_FIRST

EXEC o2c.P_CLN_SRC_DOWNLOAD_DATE @schema = 'dbo'

EXEC P_CLN_LOAD_DETAILS
----------------------------------------------------------------------------

EXEC P_CLN_ADRC 

EXEC P_CLN_BKPF

EXEC P_CLN_BSIK

EXEC P_CLN_EBAN

EXEC P_CLN_EKBE

EXEC P_CLN_EKKO

EXEC P_CLN_OVERDUE_REASON

EXEC P_CLN_PAYMENT_CALENDAR

EXEC P_CLN_T001S

EXEC P_CLN_T024 

EXEC P_CLN_VF_KRED

EXEC P_CLN_OCRLOG

EXEC P_CLN_T001

EXEC P_CLN_BSAK

EXEC P_CLN_REGUP

EXEC P_CLN_TS_INVOICES

--LAST   ---------------------------------------------------------------------------

EXEC o2c.P_CLN_CLEAN_COLUMNS @schema  = 'dbo'
GO
