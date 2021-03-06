/****** Object:  Table [dbo].[CLN_EKKO]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_EKKO](
	[CLIENT] [nvarchar](max) NULL,
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[PURCHASING_GROUP] [nvarchar](max) NULL,
	[PO_CREATED_BY] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
