/****** Object:  Table [dbo].[CLN_VF_KRED]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_VF_KRED](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[VENDOR_NAME1] [nvarchar](max) NULL,
	[VENDOR_NAME2] [nvarchar](max) NULL,
	[ADDRESS_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[ACCOUNT_GROUP] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
