/****** Object:  StoredProcedure [o2c].[p_tp10_customer]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp10_customer] as

drop table if exists o2c.tp1_customer

select cust.*, 
	   cust_m.customer_country, 
	   cust_m.customer_name1,
	   cust_m.customer_name2, 
	   cust_m.trading_partner, 
	   busab.accounting_clerk_name, 
	   busab.accounting_clerk_user, 
	   adrc.name1 as customer_name_chinese,
	   knkk.credit_control_area, 
	   knkk.credit_limit, 
	   knkk.credit_account, 
	   knkk.risk_category, 
	   knkk.block_indicator as credit_block,
	   knkk.last_internal_review, 
	   knkk.credit_reporting_group 
into o2c.tp1_customer
from o2c.cln_knb1 as cust 
	 left outer join  
	 o2c.cln_kna1 as cust_m on 
	 cust.customer_number = cust_m.customer_number
	 left outer join 
	 o2c.cln_t001s as busab on 
	 cust.accounting_clerk = busab.accounting_clerk_number and 
	 cust.company_code = busab.company_code
	 left outer join
	 o2c.cln_adrc as adrc on 
	 cust_m.address_number = adrc.address_number 
	 left outer join 
	 o2c.cln_knkk as knkk on 
	 cust.customer_number = knkk.customer_number
	 and credit_control_area = '0083'
	 left outer join 
	 o2c.cln_t001 as bukrs on 
	 cust.company_code = bukrs.company_code

update o2c.tp1_customer set customer_name_chinese = concat(customer_name1,' ', customer_name2)
where customer_name_chinese is null
GO
