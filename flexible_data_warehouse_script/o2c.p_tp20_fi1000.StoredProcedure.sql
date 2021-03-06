/****** Object:  StoredProcedure [o2c].[p_tp20_fi1000]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_tp20_fi1000]
AS
BEGIN
--Statisit table eflowclr
	drop  table if exists o2c.tp2_fi1000;

	select 
	b.*, 
	a.business_division,
	a.business_unit,
	a.division,
	a.postdate,
	a.salesamount,
    a.aramount,
	a.overdueamount
	into o2c.tp2_fi1000 from   
	(
	select  substring(company_code,9,len(company_code)-8) as company_code_a, 
	right(concat('0000000000',substring(customer_number,9,len(customer_number)-8)),10) as customer_number_a, 
	business_division,business_unit,
	substring(credit_control_area,9,len(credit_control_area)-8) as credit_control_area_a,
	division,postdate,aramount,salesamount,overdueamount
	from o2c.cln_fi1000 
	) a
	left join 
	o2c.tp1_customer b
	on a.company_code_a  = b.company_code
	and a.customer_number_a = b.customer_number

END
GO
