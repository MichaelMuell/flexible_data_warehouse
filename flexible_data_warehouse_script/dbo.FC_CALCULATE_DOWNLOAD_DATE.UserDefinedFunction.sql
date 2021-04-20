/****** Object:  UserDefinedFunction [dbo].[FC_CALCULATE_DOWNLOAD_DATE]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[FC_CALCULATE_DOWNLOAD_DATE]
(@path varchar)
RETURNS date
AS
BEGIN
DECLARE @duedate date

SET @duedate = CONVERT(date,LEFT(RIGHT(@path,30),8),112)

RETURN @duedate

END
GO
