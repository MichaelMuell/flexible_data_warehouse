/****** Object:  StoredProcedure [o2c].[p_sta_all_cust_items]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_all_cust_items] as

drop table if exists o2c.sta_all_cust_items

declare @max_clearing_date as date 

set @max_clearing_date = (select eomonth(max(clearing_date)) from o2c.tp3_all_cust_items)

select 
company_code,
document_number,
fiscal_year,
line_item,
customer_number,
special_gl_indicator,
clearing_date,
clearing_document,
assignment_number,
posting_date,
document_date,
entry_date,
currency,
reference,
document_type,
posting_key,
debit_credit,
amount_local,
amount_tax,
item_text,
gl_account,
baseline_date,
payment_terms,
payment_block,
follow_on_doc,
dunning_block,
dunning_key,
dunning_date_last,
dunning_level,
dunning_area,
billing_document,
credit_control_area,
days1,
days2,
days3,
reference_key1,
file_path,
transaction_key,
download_date,
src_download_date,
document_posted_by,
accounting_clerk,
reconciliation_account,
customer_country,
customer_name1,
customer_name2,
trading_partner,
accounting_clerk_name,
accounting_clerk_user,
customer_name_chinese,
credit_limit,
credit_account,
risk_category,
credit_block,
last_internal_review,
credit_reporting_group,
dispute_object_type,
dispute_coordinator,
dispute_process_deadline,
dispute_detailed_cause,
dispute_case_id,
dispute_case_type,
dispute_case_title,
dispute_planned_close_date,
dispute_reason,
dispute_status,
dispute_responsible,
dispute_processor,
dispute_created_on,
dispute_closed_on,
dispute_changed_on,
key_date,
due_date,
arrears_after_net,
amount_eur,
due_date_vat,
arrears_after_net_vat,
overdue_rank,
overdue_value,
relevant_for_payment_behavior,
amount_document, 
posting_to_clearing_days, 
vat_issued, 
reason,
reason_details, 
days1_vat, 
days2_vat,
reverse_document, 
overdue_rank_vat,
sample_order
into o2c.sta_all_cust_items
from o2c.tp3_all_cust_items

delete from o2c.sta_all_cust_items where 
clearing_date <= dateadd(month,-1,dateadd(year,-1,@max_clearing_date)) and clearing_date is not null 

exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_all_cust_items', @schema = 'o2c'
GO
