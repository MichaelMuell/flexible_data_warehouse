/****** Object:  Table [o2c].[cln_knb1]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_knb1](
	[company_code] [nvarchar](max) NULL,
	[customer_number] [nvarchar](max) NULL,
	[accounting_clerk] [nvarchar](max) NULL,
	[reconciliation_account] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
