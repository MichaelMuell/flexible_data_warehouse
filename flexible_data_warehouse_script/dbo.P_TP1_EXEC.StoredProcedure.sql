/****** Object:  StoredProcedure [dbo].[P_TP1_EXEC]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[P_TP1_EXEC] AS

--TP1_EKBE-------------------------------------------------------------------------------------------

EXEC P_TP10_EKBE

--TP1_VENDOR_DIMENSION---------------------------------------------------------------------------

EXEC P_TP10_VENDOR_DIM

-- TP1_IRB Table --------------------------------------------------------------------------------

EXEC P_TP10_IRB
GO
