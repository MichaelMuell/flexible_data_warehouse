/****** Object:  Table [o2c].[tp3_all_cust_items]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp3_all_cust_items](
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
	[transaction_key] [nvarchar](50) NOT NULL,
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
	[reverse_document] [nvarchar](max) NULL,
	[sample_order] [nvarchar](max) NULL,
	[key_date] [date] NULL,
	[company_code_currency] [varchar](max) NULL,
	[due_date] [varchar](max) NULL,
	[arrears_after_net] [int] NULL,
	[amount_eur] [varchar](max) NULL,
	[due_date_vat] [date] NULL,
	[arrears_after_net_vat] [int] NULL,
	[overdue_rank] [varchar](max) NULL,
	[overdue_value] [varchar](max) NULL,
	[relevant_for_payment_behavior] [varchar](max) NULL,
	[days1_vat] [int] NULL,
	[days2_vat] [int] NULL,
	[posting_to_clearing_days] [int] NULL,
	[vat_issued] [varchar](max) NULL,
	[overdue_rank_vat] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[transaction_key] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [i1]    Script Date: 4/16/2021 11:35:28 AM ******/
CREATE UNIQUE NONCLUSTERED INDEX [i1] ON [o2c].[tp3_all_cust_items]
(
	[transaction_key] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
/****** Object:  Index [i2]    Script Date: 4/16/2021 11:35:28 AM ******/
CREATE NONCLUSTERED INDEX [i2] ON [o2c].[tp3_all_cust_items]
(
	[arrears_after_net] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
/****** Object:  Index [i3]    Script Date: 4/16/2021 11:35:28 AM ******/
CREATE NONCLUSTERED INDEX [i3] ON [o2c].[tp3_all_cust_items]
(
	[arrears_after_net_vat] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
