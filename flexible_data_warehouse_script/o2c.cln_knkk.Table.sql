/****** Object:  Table [o2c].[cln_knkk]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_knkk](
	[customer_number] [nvarchar](max) NULL,
	[credit_control_area] [nvarchar](max) NULL,
	[credit_limit] [decimal](15, 2) NULL,
	[credit_account] [nvarchar](max) NULL,
	[risk_category] [nvarchar](max) NULL,
	[block_indicator] [nvarchar](max) NULL,
	[last_internal_review] [datetime2](7) NULL,
	[credit_reporting_group] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
