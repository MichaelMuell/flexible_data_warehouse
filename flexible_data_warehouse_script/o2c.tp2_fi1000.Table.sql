/****** Object:  Table [o2c].[tp2_fi1000]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[tp2_fi1000](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[salesamount] [float] NOT NULL,
	[aramount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
