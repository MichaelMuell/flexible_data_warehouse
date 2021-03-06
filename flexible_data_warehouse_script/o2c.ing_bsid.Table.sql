/****** Object:  Table [o2c].[ing_bsid]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_bsid](
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
	[reverse_document] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[transaction_key] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
