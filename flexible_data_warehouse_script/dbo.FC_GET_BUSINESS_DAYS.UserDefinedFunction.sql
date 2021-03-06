/****** Object:  UserDefinedFunction [dbo].[FC_GET_BUSINESS_DAYS]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FC_GET_BUSINESS_DAYS]
(@from datetime, @to datetime)
RETURNS int
AS
BEGIN

DECLARE @days int

IF @from = '' OR @to = ''
BEGIN  
	SET @days = 0
END 
ELSE 
BEGIN 
	SELECT @days = count(*)+1
	from CLN_PAYMENT_CALENDAR 
	where datepart(dw, DATES) not in (1,7)   
	and CHINA_PUBLIC_HOLIDAY IS NULL
	and DATES > @from and DATES <= @to
END

return ( @days )
END
GO
