/****** Object:  UserDefinedFunction [o2c].[fc_cal_eflow_duration]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [o2c].[fc_cal_eflow_duration]
(@from datetime, @to datetime)
RETURNS int
AS
BEGIN

DECLARE @days int
declare @durationmin int
declare @starttime time
declare @endtime time

IF @from = '' OR @to = ''
	BEGIN  
		SET @days = 0
		set @durationmin = 0
	END 
ELSE 
	BEGIN 
		set @days = dbo.FC_GET_BUSINESS_DAYS(@from,@to) - 1 
		set  @starttime = cast(@from as time(0))
		set  @endtime = cast(@to as time(0))
	-- only consider working time from 08:30 to 17:30
		if @starttime >= cast('00:00:00' as time(0)) and  @starttime <= cast('08:30:00' as time(0)) 
		begin
			set @starttime = cast('08:30:00' as time(0))
		end
		if @endtime >= cast('00:00:00' as time(0)) and  @endtime <= cast('08:30:00' as time(0)) 
		begin
			set @endtime = cast('08:30:00' as time(0))
		end
	
	-- set max time to 17:30:00
		if @starttime >= cast('17:30:00' as time(0)) and  @starttime <= cast('23:59:59' as time(0)) 
		begin
			set @starttime = cast('17:30:00' as time(0))
		end
		if @endtime >= cast('17:30:00' as time(0)) and  @endtime <= cast('23:59:59' as time(0)) 
		begin
			set @endtime = cast('17:30:00' as time(0))
		end
	
	    set @durationmin = DATEDIFF(MINUTE,@starttime,@endtime) + @days * 540


	END

return ( @durationmin )
END 
GO
