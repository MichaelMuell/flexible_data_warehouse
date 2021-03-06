/****** Object:  StoredProcedure [o2c].[p_tp30_all_cust_items]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp30_all_cust_items] as 

update o2c.tp3_all_cust_items set key_date = eomonth(src_download_date,-1)

update o2c.tp3_all_cust_items set due_date = o2c.fc_calculate_due_date_ar(convert(date, baseline_date,104),convert(int,days1),convert(int,days2), debit_credit,follow_on_doc) 

update o2c.tp3_all_cust_items set arrears_after_net = dbo.fc_calculate_arrears(due_date,key_date)
where clearing_date is null 

update o2c.tp3_all_cust_items set arrears_after_net = dbo.fc_calculate_arrears(due_date,clearing_date)
where clearing_date is not null 

update o2c.tp3_all_cust_items set amount_local = amount_local * -1 where debit_credit = 'H'

update o2c.tp3_all_cust_items set amount_tax = amount_tax * -1 where debit_credit = 'H'

update o2c.tp3_all_cust_items set amount_document  = amount_document * -1 where debit_credit = 'H'

update o2c.tp3_all_cust_items set company_code_currency = 'HKD' where company_code = '0078'

update o2c.tp3_all_cust_items set company_code_currency = 'CNY' where company_code <> '0078'

update o2c.tp3_all_cust_items set amount_eur = o2c.fc_convert_currency(company_code_currency,convert(decimal(30,2),amount_local), 'EUR') 

update o2c.tp3_all_cust_items set dispute_created_on = LEFT(dispute_created_on,8)

update o2c.tp3_all_cust_items set dispute_changed_on = LEFT(dispute_changed_on,8)

update o2c.tp3_all_cust_items set dispute_closed_on = LEFT(dispute_closed_on,8)

update o2c.tp3_all_cust_items set dispute_closed_on = null 
where left(dispute_closed_on,1) <> '2' and 
	  left(dispute_closed_on,1) <> ''

update o2c.tp3_all_cust_items set reference_key1 = '' 
where left(reference_key1,1) <> '2' and 
	  left(reference_key1,1) <> ''

update o2c.tp3_all_cust_items set days1_vat = o2c.fc_calculate_days1(payment_terms, reference_key1)
where payment_terms <> '' and  
	  reference_key1 <> '' 

update o2c.tp3_all_cust_items set days1_vat = days1
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set days2_vat = o2c.fc_calculate_days2(payment_terms, reference_key1)
where payment_terms <> '' and
	  reference_key1 <> ''

update o2c.tp3_all_cust_items set days2_vat = days2
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set due_date_vat = o2c.fc_calculate_due_date_ar(convert(date, reference_key1,112),days1_vat,days2_vat, debit_credit,follow_on_doc)
where payment_terms <> '' and
	  reference_key1 <> ''

update o2c.tp3_all_cust_items set due_date_vat = due_date
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set arrears_after_net_vat = dbo.fc_calculate_arrears(due_date_vat,key_date) 
where payment_terms <> '' and
	  reference_key1 <> '' and 
	  clearing_date is null 

update o2c.tp3_all_cust_items set arrears_after_net_vat = dbo.fc_calculate_arrears(due_date_vat,clearing_date) 
where payment_terms <> '' and
	  reference_key1 <> '' and 
	  clearing_date is not null

update o2c.tp3_all_cust_items set arrears_after_net_vat = arrears_after_net
where payment_terms = '' or  
	  reference_key1 = ''

update o2c.tp3_all_cust_items set posting_to_clearing_days = dbo.FC_GET_BUSINESS_DAYS(CONVERT(date, posting_date,104), CONVERT(date, clearing_date,104)) 

update o2c.tp3_all_cust_items set relevant_for_payment_behavior = 'X' 
where 
	(company_code = '0078' 
	 and debit_credit = 'S') 
	 or 
	(company_code = '0083'  
	 and debit_credit = 'S'
	 and not contains(item_text, 'quality')
	 and not contains(item_text, 'price')
	 and not contains(item_text, 'write')
	 and not contains(item_text, 'sample'))
	 or 
	 (company_code = '0289'
	 and debit_credit = 'S') 
	 or 
	 (company_code = '0369'
	 and debit_credit = 'S') 
	 or 
	 (company_code = '0199' 
	 and debit_credit = 'S' 
	 and reference <> LEFT('INV.',4)
	 and not contains(item_text, 'price')
	 and not contains(item_text, 'deduction') 
	 and not contains(item_text, '保证金') 
	 and not contains(item_text, '质量')
	 and not contains(item_text, '质保金')
	 and not contains(item_text, '三包')
	 and not contains(item_text, '扣款')
	 and not contains(item_text, '折扣')
	 and not contains(item_text, '折让')
	 and not contains(item_text, '税'))

update o2c.tp3_all_cust_items set relevant_for_payment_behavior = '' 
where reverse_document = 'X' and MONTH(key_date) < MONTH(clearing_date)

update o2c.tp3_all_cust_items set overdue_rank = '1-30'
where arrears_after_net > 0 and arrears_after_net <= 30

update o2c.tp3_all_cust_items set overdue_rank = '31-90'
where arrears_after_net > 30 and arrears_after_net <= 90

update o2c.tp3_all_cust_items set overdue_rank = '90+'
where arrears_after_net > 90

update o2c.tp3_all_cust_items set overdue_rank = 'not_due'
where arrears_after_net <= 0

update o2c.tp3_all_cust_items set overdue_rank_vat = '1-30'
where arrears_after_net_vat > 0 and arrears_after_net_vat <= 30

update o2c.tp3_all_cust_items set overdue_rank_vat = '31-90'
where arrears_after_net_vat > 30 and arrears_after_net_vat <= 90

update o2c.tp3_all_cust_items set overdue_rank_vat = '90+'
where arrears_after_net_vat > 90

update o2c.tp3_all_cust_items set overdue_rank_vat = 'not_due'
where arrears_after_net_vat <= 0

update o2c.tp3_all_cust_items set overdue_value = amount_local
where relevant_for_payment_behavior = 'X' 

update o2c.tp3_all_cust_items set vat_issued = 'VAT issued' where left(REFERENCE,3) = 'INV'
update o2c.tp3_all_cust_items set vat_issued = 'no VAT' where left(REFERENCE,3) <> 'INV'
GO
