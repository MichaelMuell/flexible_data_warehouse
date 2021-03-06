/****** Object:  Table [dbo].[ING_BKPF]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_BKPF](
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[FISCAL_YEAR] [decimal](4, 0) NULL,
	[DOCUMENT_POSTED_BY] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
