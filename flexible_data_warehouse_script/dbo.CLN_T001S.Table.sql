/****** Object:  Table [dbo].[CLN_T001S]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_T001S](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NUMBER] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_NAME] [nvarchar](max) NULL,
	[ACCOUNTING_CLERK_USER] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
