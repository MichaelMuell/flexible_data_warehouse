/****** Object:  StoredProcedure [o2c].[p_tp20_all_cust_items]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp20_all_cust_items] as 

drop table if exists o2c.tp2_all_cust_items

select items.*, 
cust.accounting_clerk,
cust.reconciliation_account,
cust.customer_country,
cust.customer_name1,
cust.customer_name2,
cust.trading_partner,
cust.accounting_clerk_name,
cust.accounting_clerk_user,
cust.customer_name_chinese,
cust.credit_limit,
cust.credit_account,
cust.risk_category,
cust.credit_block,
cust.last_internal_review,
cust.credit_reporting_group,
disp.dispute_object_type,
disp.dispute_coordinator,
disp.dispute_process_deadline,
disp.dispute_detailed_cause,
disp.dispute_case_id,
disp.dispute_case_type,
disp.dispute_case_title,
disp.dispute_planned_close_date,
disp.dispute_reason,
disp.dispute_status,
disp.dispute_responsible,
disp.dispute_processor,
disp.dispute_created_on,
disp.dispute_closed_on,
disp.dispute_changed_on, 
sam.sample_order,
reason.reason,
reason.reason_details
into o2c.tp2_all_cust_items 
from o2c.tp1_all_cust_items as items 
	 left outer join o2c.tp1_customer as cust on
	 items.customer_number = cust.customer_number and 
	 items.company_code = cust.company_code 
	 left outer join o2c.tp1_dispute as disp on 
	 items.transaction_key = disp.transaction_key
	 left outer join o2c.cln_sample_orders as sam on
	 items.company_code = sam.company_code and 
	 items.document_number = sam.document_number and 
	 items.line_item = sam.line_item and 
	 items.fiscal_year = sam.[year] 
	 left outer join dbo.cln_overdue_reason as reason on 
	 items.company_code = reason.company_code and 
	 items.document_number = reason.document_number and 
	 items.line_item = reason.line_item and 
	 items.fiscal_year = reason.[year] 
GO
