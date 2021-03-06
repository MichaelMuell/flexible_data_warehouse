/****** Object:  StoredProcedure [o2c].[p_tp30_first]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp30_first] as

drop table if exists o2c.tp3_fi1000

select * into o2c.tp3_fi1000
from o2c.tp2_fi1000

if col_length ('o2c.tp3_fi1000','payment_behavior_ar_amount') is null
begin
	alter table o2c.tp3_fi1000
	add payment_behavior_ar_amount float
end

if col_length ('o2c.tp3_fi1000','sum_overdue_0_30') is null
begin
	alter table o2c.tp3_fi1000
	add sum_overdue_0_30 float
end

if col_length ('o2c.tp3_fi1000','sum_overdue_30_60') is null
begin
	alter table o2c.tp3_fi1000
	add sum_overdue_30_60 float
end

if col_length ('o2c.tp3_fi1000','sum_overdue_60') is null
begin
	alter table o2c.tp3_fi1000
	add sum_overdue_60 float
end

if col_length ('o2c.tp3_fi1000','receiveables_0_30') is null
begin
	alter table o2c.tp3_fi1000
	add receiveables_0_30 varchar(max)
end

if col_length ('o2c.tp3_fi1000','receiveables_30_60') is null
begin
	alter table o2c.tp3_fi1000
	add receiveables_30_60 varchar(max)
end

if col_length ('o2c.tp3_fi1000','receiveables_60') is null
begin
	alter table o2c.tp3_fi1000
	add receiveables_60 varchar(max)
end

drop table if exists o2c.tp3_all_cust_items 

create table [o2c].[tp3_all_cust_items](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[line_item] [decimal](3, 0) NULL,
	[customer_number] [nvarchar](max) NULL,
	[special_gl_indicator] [nvarchar](max) NULL,
	[clearing_date] [datetime2](7) NULL,
	[clearing_document] [nvarchar](max) NULL,
	[assignment_number] [nvarchar](max) NULL,
	[posting_date] [datetime2](7) NULL,
	[document_date] [datetime2](7) NULL,
	[entry_date] [datetime2](7) NULL,
	[currency] [nvarchar](max) NULL,
	[reference] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[posting_key] [nvarchar](max) NULL,
	[debit_credit] [nvarchar](max) NULL,
	[amount_local] [decimal](13, 2) NULL,
	[amount_tax] [decimal](13, 2) NULL,
	[item_text] [nvarchar](max) NULL,
	[gl_account] [nvarchar](max) NULL,
	[baseline_date] [datetime2](7) NULL,
	[payment_terms] [nvarchar](max) NULL,
	[payment_block] [nvarchar](max) NULL,
	[follow_on_doc] [nvarchar](max) NULL,
	[dunning_block] [nvarchar](max) NULL,
	[dunning_key] [nvarchar](max) NULL,
	[dunning_date_last] [datetime2](7) NULL,
	[dunning_level] [decimal](1, 0) NULL,
	[dunning_area] [nvarchar](max) NULL,
	[billing_document] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[days1] [decimal](3, 0) NULL,
	[days2] [decimal](3, 0) NULL,
	[days3] [decimal](3, 0) NULL,
	[reference_key1] [nvarchar](max) NULL,
	[amount_document] [decimal](13, 2) NULL,
	[file_path] [nvarchar](max) NULL,
	transaction_key nvarchar(50) not null,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[dispute_object_type] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[reason] [nvarchar](max) NULL,
	[reason_details] [nvarchar](max) NULL,
	[reverse_document][nvarchar](max) NULL,
	[sample_order][nvarchar](max) NULL
		primary key (transaction_key)
) on [primary] textimage_on [primary]

create unique index i1 on o2c.tp3_all_cust_items(transaction_key);

create fulltext index on  o2c.tp3_all_cust_items (
    item_text language 0
) key index i1
with 
    change_tracking = auto, 
    stoplist=off
;

insert into [o2c].[tp3_all_cust_items]
           ([company_code]
           ,[document_number]
           ,[fiscal_year]
           ,[line_item]
           ,[customer_number]
           ,[special_gl_indicator]
           ,[clearing_date]
           ,[clearing_document]
           ,[assignment_number]
           ,[posting_date]
           ,[document_date]
           ,[entry_date]
           ,[currency]
           ,[reference]
           ,[document_type]
           ,[posting_key]
           ,[debit_credit]
           ,[amount_local]
           ,[amount_tax]
           ,[item_text]
           ,[gl_account]
           ,[baseline_date]
           ,[payment_terms]
           ,[payment_block]
           ,[follow_on_doc]
           ,[dunning_block]
           ,[dunning_key]
           ,[dunning_date_last]
           ,[dunning_level]
           ,[dunning_area]
           ,[billing_document]
           ,[credit_control_area]
           ,[days1]
           ,[days2]
           ,[days3]
           ,[reference_key1]
           ,[amount_document]
           ,[file_path]
           ,[transaction_key]
           ,[download_date]
           ,[src_download_date]
           ,[document_posted_by]
           ,[accounting_clerk]
           ,[reconciliation_account]
           ,[customer_country]
           ,[customer_name1]
           ,[customer_name2]
           ,[trading_partner]
           ,[accounting_clerk_name]
           ,[accounting_clerk_user]
           ,[customer_name_chinese]
           ,[credit_limit]
           ,[credit_account]
           ,[risk_category]
           ,[credit_block]
           ,[last_internal_review]
           ,[credit_reporting_group]
           ,[dispute_object_type]
           ,[dispute_coordinator]
           ,[dispute_process_deadline]
           ,[dispute_detailed_cause]
           ,[dispute_case_id]
           ,[dispute_case_type]
           ,[dispute_case_title]
           ,[dispute_planned_close_date]
           ,[dispute_reason]
           ,[dispute_status]
           ,[dispute_responsible]
           ,[dispute_processor]
           ,[dispute_created_on]
           ,[dispute_closed_on]
           ,[dispute_changed_on]
		   ,[reason]
		   ,[reason_details]
		   ,[reverse_document]
		   ,[sample_order])
select 
            [company_code]
           ,[document_number]
           ,[fiscal_year]
           ,[line_item]
           ,[customer_number]
           ,[special_gl_indicator]
           ,[clearing_date]
           ,[clearing_document]
           ,[assignment_number]
           ,[posting_date]
           ,[document_date]
           ,[entry_date]
           ,[currency]
           ,[reference]
           ,[document_type]
           ,[posting_key]
           ,[debit_credit]
           ,[amount_local]
           ,[amount_tax]
           ,[item_text]
           ,[gl_account]
           ,[baseline_date]
           ,[payment_terms]
           ,[payment_block]
           ,[follow_on_doc]
           ,[dunning_block]
           ,[dunning_key]
           ,[dunning_date_last]
           ,[dunning_level]
           ,[dunning_area]
           ,[billing_document]
           ,[credit_control_area]
           ,[days1]
           ,[days2]
           ,[days3]
           ,[reference_key1]
           ,[amount_document]
           ,[file_path]
           ,[transaction_key]
           ,[download_date]
           ,[src_download_date]
           ,[document_posted_by]
           ,[accounting_clerk]
           ,[reconciliation_account]
           ,[customer_country]
           ,[customer_name1]
           ,[customer_name2]
           ,[trading_partner]
           ,[accounting_clerk_name]
           ,[accounting_clerk_user]
           ,[customer_name_chinese]
           ,[credit_limit]
           ,[credit_account]
           ,[risk_category]
           ,[credit_block]
           ,[last_internal_review]
           ,[credit_reporting_group]
           ,[dispute_object_type]
           ,[dispute_coordinator]
           ,[dispute_process_deadline]
           ,[dispute_detailed_cause]
           ,[dispute_case_id]
           ,[dispute_case_type]
           ,[dispute_case_title]
           ,[dispute_planned_close_date]
           ,[dispute_reason]
           ,[dispute_status]
           ,[dispute_responsible]
           ,[dispute_processor]
           ,[dispute_created_on]
           ,[dispute_closed_on]
           ,[dispute_changed_on]
		   ,[reason]
		   ,[reason_details]
		   ,[reverse_document]
		   ,[sample_order]
from o2c.tp2_all_cust_items

if col_length ('o2c.tp3_all_cust_items','key_date') is null
begin
	alter table o2c.tp3_all_cust_items
	add key_date date
end

if col_length ('o2c.tp3_all_cust_items','company_code_currency') is null
begin
	alter table o2c.tp3_all_cust_items
	add company_code_currency varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','due_date') is null 
begin
	alter table o2c.tp3_all_cust_items
	add due_date varchar(max)
end 

if col_length ('o2c.tp3_all_cust_items','arrears_after_net') is null 
begin
	alter table o2c.tp3_all_cust_items
	add arrears_after_net int
	create index i2 on o2c.tp3_all_cust_items(arrears_after_net);
end

if col_length ('o2c.tp3_all_cust_items','amount_eur') is null
begin
	alter table o2c.tp3_all_cust_items
	add amount_eur varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','amount_eur') is null
begin
	alter table o2c.tp3_all_cust_items
	add amount_eur varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','due_date_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add due_date_vat date
end

if col_length ('o2c.tp3_all_cust_items','arrears_after_net_vat') is null 
begin
	alter table o2c.tp3_all_cust_items
	add arrears_after_net_vat int 
	create index i3 on o2c.tp3_all_cust_items(arrears_after_net_vat);
end

if col_length ('o2c.tp3_all_cust_items','overdue_rank') is null 
begin
	alter table o2c.tp3_all_cust_items
	add overdue_rank varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','overdue_value') is null 
begin
	alter table o2c.tp3_all_cust_items
	add overdue_value varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','relevant_for_payment_behavior') is null 
begin
	alter table o2c.tp3_all_cust_items
	add relevant_for_payment_behavior varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','days1_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add days1_vat int
end

if col_length ('o2c.tp3_all_cust_items','days2_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add days2_vat int
end

if col_length ('o2c.tp3_all_cust_items','posting_to_clearing_days') is null
begin
	alter table o2c.tp3_all_cust_items
	add posting_to_clearing_days int
end

if col_length ('o2c.tp3_all_cust_items','vat_issued') is null
begin
	alter table o2c.tp3_all_cust_items
	add vat_issued varchar(max)
end

if col_length ('o2c.tp3_all_cust_items','overdue_rank_vat') is null
begin
	alter table o2c.tp3_all_cust_items
	add overdue_rank_vat varchar(max)
end

GO
