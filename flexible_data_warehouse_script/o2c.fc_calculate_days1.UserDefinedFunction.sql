/****** Object:  UserDefinedFunction [o2c].[fc_calculate_days1]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE function [o2c].[fc_calculate_days1]
(@payment_terms nvarchar(max), @baseline_date nvarchar(max))
returns int
as
begin

--declare @payment_terms as nvarchar(max)
--declare @baseline_date as nvarchar(max)
declare @days1 as int
declare @days1_fixed as int 
declare @due_date_special1 as int
declare @month_special1 as int 
declare @date as date 
declare @day_limit as int

--set @payment_terms = 'Z304'
--set @baseline_date = '2021-01-09 00:00:00.0000000'

set @day_limit = (select min(day_limit)
					from o2c.cln_t052 
					where payment_term = @payment_terms)

set @baseline_date = try_convert(date,@baseline_date)
set @days1 = 0

if @day_limit = 0
begin 
	set @days1_fixed = (select max(days1_fixed)
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @due_date_special1 = (
						select max(due_date_special1) 
						from o2c.cln_t052 
						where payment_term = @payment_terms)

	set @month_special1 = (
						select max(month_special1)
						from o2c.cln_t052 
						where payment_term = @payment_terms)
end 
else 
begin
	set @days1_fixed = (select top 1 days1_fixed
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2) 
						order by day_limit)

	set @due_date_special1 = (
						select top 1 due_date_special1
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)
	
	set @month_special1 = (
						select top 1 month_special1
						from o2c.cln_t052 
						where payment_term = @payment_terms and 
						day_limit >= right(@baseline_date,2)
						order by day_limit)
end

set @days1 = @days1_fixed

if @due_date_special1 > 0 or @month_special1 > 0
begin
	set @date = @baseline_date

	if @days1_fixed > 0 
	begin
	    set @date =  dateadd(day,@days1_fixed,convert(date,@baseline_date))
	end 
--		date calculation 
	set @date = DATEADD(MONTH,@month_special1,@date)
	
	if @due_date_special1 = 31
	begin 
		set @due_date_special1 = right(EOMONTH(@date),2)
	end

	set @date = DATEFROMPARTS(left(@date,4),right(left(@date,7),2),@due_date_special1)

	if  @date < @baseline_date 
	begin	
		set	@baseline_date = @date 
	end 

	set @days1 = DATEDIFF(DAY,@baseline_date,@date)

end
return @days1
end


GO
