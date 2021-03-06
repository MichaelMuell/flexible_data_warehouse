/****** Object:  StoredProcedure [o2c].[p_sta_payment_behavior]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_payment_behavior] as 
begin

drop table if exists o2c.sta_payment_behavior
drop table if exists #temp_od
drop table if exists #temp_sum 
drop table if exists #temp_cust;

with sum_od as (
    select distinct credit_account,
		   key_date,
		   overdue_rank_vat,
		   sum(convert(float,overdue_value) ) over (
            partition by 
				convert(varchar(10),credit_account), 
				key_date,
				convert(varchar(10),overdue_rank_vat) 
            order by 
                convert(varchar(10),credit_account),
				key_date ,
				convert(varchar(10),overdue_rank)  
			) overdue_value_by_ca 
     from 
        o2c.sta_open_cust_items
	 where credit_account is not null 
	 and relevant_for_payment_behavior = 'x')


select * into #temp_od from (
  select credit_account, key_date, overdue_rank_vat,overdue_value_by_ca
  from sum_od
) t
pivot (
  sum(overdue_value_by_ca)
  for overdue_rank_vat in (
   [1-30], [90+], [31-90], [not_due]
  ) 
)as p;


with sum_sales as (
    select distinct credit_account,
		   sum(convert(float,amount_local) ) over (
            partition by 
				convert(varchar(10),credit_account)
            order by 
                convert(varchar(10),credit_account)
			) sales_by_ca
     from 
        o2c.sta_all_cust_items
	 where (document_type = 'dg' or 
		   document_type = 'dr') and 
		   posting_date between eomonth(dateadd(month,-12,key_date)) and key_date)

select * into #temp_sum 
from sum_sales

select * into #temp_cust 
from o2c.tp1_customer 
where customer_number = credit_account;

with del_cust_duplicates as (
    select 
        *,
        row_number() over (
            partition by 
				credit_account
            order by 
				credit_account,
				company_code 
        ) row_num
     from 
        #temp_cust
)

delete from del_cust_duplicates
where row_num > 1

select od.credit_account, 
	   od.key_date, 
	   od.[1-30], 
	   od.[31-90], 
	   od.[90+],
	   od.[not_due],
	   su.sales_by_ca, 
	   cust.credit_limit,
	   cust.credit_reporting_group,
	   cust.customer_country,
	   cust.customer_name_chinese,
	   cust.last_internal_review,
	   cust.reconciliation_account,
	   cust.risk_category,
	   cust.trading_partner,
	   cust.credit_block
into o2c.sta_payment_behavior
from #temp_od as od 
	left join 
	#temp_sum as su on 
	od.credit_account = su.credit_account 
	left join 
	#temp_cust as cust on 
	right('0000000000' + convert(varchar(10),od.credit_account), 10) = cust.credit_account

drop table if exists #temp_od
drop table if exists #temp_sum 
drop table if exists #temp_cust

end
GO
