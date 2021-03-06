/****** Object:  Table [dbo].[TP1_VENDOR_DIMENSION]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP1_VENDOR_DIMENSION](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[VENDOR_NAME] [nvarchar](max) NOT NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[RECONCILIATION_ACCOUNT] [nvarchar](max) NULL,
	[VENDOR_COUNTRY] [nvarchar](max) NULL,
	[TRADING_PARTNER] [nvarchar](max) NULL,
	[VENDOR_NAME_CHINESE] [nvarchar](max) NOT NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
