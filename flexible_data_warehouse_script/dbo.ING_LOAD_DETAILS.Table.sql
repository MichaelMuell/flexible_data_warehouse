/****** Object:  Table [dbo].[ING_LOAD_DETAILS]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_LOAD_DETAILS](
	[TABLE_NAME] [nvarchar](max) NULL,
	[SRC_DOWNLOAD_DATE] [datetime2](7) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
