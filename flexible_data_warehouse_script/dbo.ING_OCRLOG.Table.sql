/****** Object:  Table [dbo].[ING_OCRLOG]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_OCRLOG](
	[TIF_FILE] [nvarchar](max) NULL,
	[FSSC_LOCATION] [nvarchar](max) NULL,
	[TS_FILENAME] [nvarchar](max) NULL,
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
