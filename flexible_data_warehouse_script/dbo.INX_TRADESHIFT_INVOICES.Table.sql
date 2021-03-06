/****** Object:  Table [dbo].[INX_TRADESHIFT_INVOICES]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[INX_TRADESHIFT_INVOICES](
	[TS_FAPIAO_CODE] [nvarchar](max) NULL,
	[TS_REFERENCE] [nvarchar](max) NULL,
	[TS_PO_REMARK] [nvarchar](max) NULL,
	[TS_ERROR_01] [nvarchar](max) NULL,
	[TS_ERROR_02] [nvarchar](max) NULL,
	[TS_ERROR_03] [nvarchar](max) NULL,
	[TS_ERROR_04] [nvarchar](max) NULL,
	[TS_ERROR_05] [nvarchar](max) NULL,
	[TS_ERROR_06] [nvarchar](max) NULL,
	[TS_ERROR_07] [nvarchar](max) NULL,
	[TS_ERROR_08] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
