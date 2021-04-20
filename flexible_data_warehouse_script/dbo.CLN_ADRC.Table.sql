/****** Object:  Table [dbo].[CLN_ADRC]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_ADRC](
	[ADDRESS_NUMBER] [nvarchar](max) NULL,
	[NATION] [nvarchar](max) NULL,
	[NAME1] [nvarchar](max) NULL,
	[NAME2] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
