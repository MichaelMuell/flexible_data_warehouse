/****** Object:  Table [dbo].[metadata]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[metadata](
	[tablename] [nvarchar](257) NULL,
	[rowscount] [bigint] NULL,
	[colcount] [int] NULL
) ON [PRIMARY]
GO
