/****** Object:  UserDefinedFunction [dbo].[FC_CALCULATE_ARREARS]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_CALCULATE_ARREARS]
(@start date, @end date)
RETURNS int
AS
BEGIN

DECLARE @arrears int

SET @arrears = DATEDIFF(day, @start, @end)

RETURN @arrears
END
GO
