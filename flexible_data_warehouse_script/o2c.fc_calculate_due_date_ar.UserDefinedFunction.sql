/****** Object:  UserDefinedFunction [o2c].[fc_calculate_due_date_ar]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [o2c].[fc_calculate_due_date_ar]
(@start date, @days1 int, @days2 int, @debit_credit varchar(Max), @follow_on_doc varchar(Max))
RETURNS date
AS
BEGIN

DECLARE @duedate date

IF @debit_credit = 'H' AND @follow_on_doc = ''
	BEGIN
	SET @duedate = @start 
	END 
ELSE 
	BEGIN
	IF @days2 = 0 
		BEGIN 
		SET @duedate = DATEADD(day,@days1,@start) 
		END 
	ELSE
		BEGIN
		SET @duedate = DATEADD(day, @days2, @start)
		END 
END

RETURN @duedate 

END
GO
