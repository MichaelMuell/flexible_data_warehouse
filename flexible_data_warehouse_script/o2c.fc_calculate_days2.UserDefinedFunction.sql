/****** Object:  UserDefinedFunction [o2c].[fc_calculate_days2]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE function [o2c].[fc_calculate_days2]
(@payment_terms nvarchar(max), @baseline_date nvarchar(max))
returns int
as
begin

--declare @payment_terms as nvarchar(max)
--declare @baseline_date as nvarchar(max)
declare @days2 as int
declare @days2_fixed as int 
declare @due_date_special2 as int
declare @month_special2 as int 
declare @date as date 
declare @day_limit as int

--set @payment_terms = '3330'
--set @baseline_date = '2020-10-31 00:00:00.0000000'

set @day_limit = (select min(day_limit)
					from o2c.cln_t052 
					where payment_term = @payment_terms)

set @baseline_date = try_convert(date,@baseline_date)
set @days2 = 0

if @day_limit = 0
begin 
	set @days2_fixed = (select max(days2_fixed)
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @due_date_special2 = (
						select max(due_date_special2) 
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @month_special2 = (
						select max(month_special2)
						from o2c.cln_t052 
						where payment_term = @payment_terms)
end 
else 
begin
	set @days2_fixed = (select top 1 days2_fixed
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)

	set @due_date_special2 = (
						select top 1 due_date_special2 
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)

	set @month_special2 = (
						select top 1 month_special2
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)
end

set @days2 =  @days2_fixed

if @due_date_special2 > 0 or @month_special2 > 0
begin
	set @date = @baseline_date

	if @days2_fixed > 0 
	begin
	    set @date =  dateadd(day,@days2_fixed,convert(date,@baseline_date))
	end 
--		date calculation 
	set @date = DATEADD(MONTH,@month_special2,@date)
	
	if @due_date_special2 = 31
	begin 
		set @due_date_special2 = right(EOMONTH(@date),2)
	end

	set @date = DATEFROMPARTS(left(@date,4),right(left(@date,7),2),@due_date_special2)

	if  @date < @baseline_date 
	begin	
		set	@baseline_date = @date 
	end 

	set @days2 = DATEDIFF(DAY,@baseline_date,@date)

end
return @days2
end


GO
