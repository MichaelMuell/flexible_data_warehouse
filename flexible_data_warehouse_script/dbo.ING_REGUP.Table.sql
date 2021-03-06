/****** Object:  Table [dbo].[ING_REGUP]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_REGUP](
	[RUN_DATE] [date] NULL,
	[RUN_ID] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[PAYMENT_DOCUMENT] [nvarchar](max) NULL,
	[XVORL] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[FISCAL_YEAR] [decimal](4, 0) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
