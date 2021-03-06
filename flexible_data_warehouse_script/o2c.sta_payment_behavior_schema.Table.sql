/****** Object:  Table [o2c].[sta_payment_behavior_schema]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_payment_behavior_schema](
	[credit_account] [nvarchar](max) NULL,
	[key_date] [date] NULL,
	[1-30] [float] NULL,
	[31-90] [float] NULL,
	[90+] [float] NULL,
	[not_due] [float] NULL,
	[sales_by_ca] [float] NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name_chinese] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[credit_block] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
