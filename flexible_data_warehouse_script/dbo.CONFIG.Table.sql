/****** Object:  Table [dbo].[CONFIG]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CONFIG](
	[FUNCTION_NAME] [varchar](max) NULL,
	[TABLE_NAME] [varchar](max) NULL,
	[COLUMN_NAME] [varchar](max) NULL,
	[PARAMETER] [varchar](max) NULL,
	[ACTIVE] [varchar](max) NULL,
	[DB_SCHEMA] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
