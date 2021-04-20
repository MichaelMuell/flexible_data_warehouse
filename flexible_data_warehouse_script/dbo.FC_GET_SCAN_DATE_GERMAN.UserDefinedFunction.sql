/****** Object:  UserDefinedFunction [dbo].[FC_GET_SCAN_DATE_GERMAN]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_GET_SCAN_DATE_GERMAN]
(@date date, @hour int)
RETURNS date
AS
BEGIN 

IF @hour <6 
BEGIN 
SET @date = DATEADD(DAY, -1,@date)
END 

RETURN(@date)
END
GO
