/****** Object:  Table [dbo].[ING_EBAN]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_EBAN](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[REQUISITIONER] [nvarchar](max) NULL,
	[PR_CREATOR] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
