/****** Object:  StoredProcedure [dbo].[P_TP2_EXEC]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP2_EXEC] AS 

BEGIN TRANSACTION

--TP2_OPEN_ITEMS--------------------------------------------------------------------------------

EXEC P_TP20_OPEN_ITEMS

--TP2_CLEARED_ITEMS------------------------------------------------------------------------------

EXEC P_TP20_CLEARED_ITEMS


-- TP2_IRB-------------------------------------------------------------------------------
EXEC P_TP20_IRB

COMMIT TRANSACTION
GO
