/****** Object:  Table [dbo].[INX_OVERDUE_REASON]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[INX_OVERDUE_REASON](
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[YEAR] [nvarchar](max) NULL,
	[LINE_ITEM] [nvarchar](max) NULL,
	[KEY_DATE] [nvarchar](max) NULL,
	[REASON] [nvarchar](max) NULL,
	[REASON_DETAILS] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
