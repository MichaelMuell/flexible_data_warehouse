/****** Object:  StoredProcedure [o2c].[p_cln_fi1000]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_cln_fi1000] as 


declare @inyearmonth nvarchar(10)
declare @fulldate nvarchar(10)
declare @initdate date
select @inyearmonth = max(monthyearfrom) from o2c.ing_fi1000 
select @fulldate = concat(right(@inyearmonth,4),left(@inyearmonth,2),'01')
select @initdate = convert(date,@fulldate,102)

drop table if exists o2c.#bwfi1000ar
select company_code, customer_number,business_division,business_unit,credit_control_area,division,
    case yearmonth 
     when 'ar_month_0' then @initdate
	 when 'ar_month_1' then dateadd(MONTH,-1,@initdate)
	 when 'ar_month_2' then dateadd(MONTH,-2,@initdate)
	 when 'ar_month_3' then dateadd(MONTH,-3,@initdate)
	 when 'ar_month_4' then dateadd(MONTH,-4,@initdate)
	 when 'ar_month_5' then dateadd(MONTH,-5,@initdate)
	 when 'ar_month_6' then dateadd(MONTH,-6,@initdate)
	 when 'ar_month_7' then dateadd(MONTH,-7,@initdate)
	 when 'ar_month_8' then dateadd(MONTH,-8,@initdate)
	 when 'ar_month_9' then dateadd(MONTH,-9,@initdate)
	 when 'ar_month_10' then dateadd(MONTH,-10,@initdate)
	 when 'ar_month_11' then dateadd(MONTH,-11,@initdate)
	 when 'ar_month_12' then dateadd(MONTH,-12,@initdate)
   end as postdate, 'aramount' as category, amount
 into o2c.#bwfi1000ar
 from 
(
	select  company_code,customer_number,business_division,business_unit,credit_control_area,division,ar_month_0 , ar_month_1,ar_month_2,
	      ar_month_3,ar_month_4,ar_month_5,ar_month_6,ar_month_7,ar_month_8,ar_month_9,ar_month_10,ar_month_11,ar_month_12
	from o2c.ing_fi1000
)  p

unpivot (
    amount for yearmonth in (ar_month_0,ar_month_1,ar_month_2,ar_month_3,ar_month_4,ar_month_5,ar_month_6,ar_month_7,ar_month_8,ar_month_9,ar_month_10,ar_month_11,ar_month_12)
	)
 as unpvtar;



drop table if exists o2c.#bwfi1000sales
select company_code, customer_number,business_division,business_unit,credit_control_area,division,
    case yearmonth 
     when 'sales_month_0' then @initdate
	 when 'sales_month_1' then dateadd(MONTH,-1,@initdate)
	 when 'sales_month_2' then dateadd(MONTH,-2,@initdate)
	 when 'sales_month_3' then dateadd(MONTH,-3,@initdate)
	 when 'sales_month_4' then dateadd(MONTH,-4,@initdate)
	 when 'sales_month_5' then dateadd(MONTH,-5,@initdate)
	 when 'sales_month_6' then dateadd(MONTH,-6,@initdate)
	 when 'sales_month_7' then dateadd(MONTH,-7,@initdate)
	 when 'sales_month_8' then dateadd(MONTH,-8,@initdate)
	 when 'sales_month_9' then dateadd(MONTH,-9,@initdate)
	 when 'sales_month_10' then dateadd(MONTH,-10,@initdate)
	 when 'sales_month_11' then dateadd(MONTH,-11,@initdate)
	 when 'sales_month_12' then dateadd(MONTH,-12,@initdate)
   end as postdate, 'salesamount' as category,amount
 into o2c.#bwfi1000sales
 from 
(
	select  company_code,customer_number,business_division,business_unit,credit_control_area,division,sales_month_0 , sales_month_1,sales_month_2,
	      sales_month_3,sales_month_4,sales_month_5,sales_month_6,sales_month_7,sales_month_8,sales_month_9,sales_month_10,sales_month_11,sales_month_12
	from o2c.ing_fi1000
)  p

unpivot (
    amount for yearmonth in (sales_month_0,sales_month_1,sales_month_2,sales_month_3,sales_month_4,sales_month_5,sales_month_6,sales_month_7,sales_month_8,sales_month_9,sales_month_10,sales_month_11,sales_month_12)
	)
 as unpvtsales;



 -- unpivot overdue amount
drop table if exists o2c.#bwfi1000overdue
select company_code, customer_number,business_division,business_unit,credit_control_area,division,
    case yearmonth 
     when 'overdue_month_0' then @initdate
	 when 'overdue_month_1' then dateadd(MONTH,-1,@initdate)
	 when 'overdue_month_2' then dateadd(MONTH,-2,@initdate)
	 when 'overdue_month_3' then dateadd(MONTH,-3,@initdate)
	 when 'overdue_month_4' then dateadd(MONTH,-4,@initdate)
	 when 'overdue_month_5' then dateadd(MONTH,-5,@initdate)
	 when 'overdue_month_6' then dateadd(MONTH,-6,@initdate)
	 when 'overdue_month_7' then dateadd(MONTH,-7,@initdate)
	 when 'overdue_month_8' then dateadd(MONTH,-8,@initdate)
	 when 'overdue_month_9' then dateadd(MONTH,-9,@initdate)
	 when 'overdue_month_10' then dateadd(MONTH,-10,@initdate)
	 when 'overdue_month_11' then dateadd(MONTH,-11,@initdate)
	 when 'overdue_month_12' then dateadd(MONTH,-12,@initdate)
   end as postdate, 'overdueamount' as category, amount
 into o2c.#bwfi1000overdue
 from 
(
	select  company_code,customer_number,business_division,business_unit,credit_control_area,division,
	overdue_month_0 , overdue_month_1,overdue_month_2,overdue_month_3,overdue_month_4,overdue_month_5,overdue_month_6,overdue_month_7,overdue_month_8,
	overdue_month_9,overdue_month_10,overdue_month_11,overdue_month_12
	from o2c.ing_fi1000
)  p

unpivot (
    amount for yearmonth in (overdue_month_0 , overdue_month_1,overdue_month_2,overdue_month_3,overdue_month_4,overdue_month_5,overdue_month_6,
	   overdue_month_7,overdue_month_8,overdue_month_9,overdue_month_10,overdue_month_11,overdue_month_12)
	)
 as unpvtoverdue;






 drop table if exists o2c.#bwfi1000
 select *  into o2c.#bwfi1000 from o2c.#bwfi1000ar
 union all 
 select * from o2c.#bwfi1000sales
 union all 
 select * from o2c.#bwfi1000overdue


 --pivot 
 drop table if exists o2c.cln_fi1000
 select company_code,customer_number,business_division,business_unit,credit_control_area,division,postdate, 
 isnull(aramount,0) aramount ,isnull(salesamount,0) salesamount ,isnull(overdueamount,0) overdueamount
 into o2c.cln_fi1000
 from 
 (
 select company_code,customer_number,business_division,business_unit,credit_control_area,division,postdate,category,amount from o2c.#bwfi1000
 )p
  pivot 
 (
    sum(amount) for category in (aramount,salesamount,overdueamount) 
 ) as pvt

  order by  postdate

   drop table if exists o2c.#bwfi1000ar
   drop table if exists o2c.#bwfi1000sales
   drop table if exists o2c.#bwfi1000overdue
   drop table if exists o2c.#bwfi1000


GO
