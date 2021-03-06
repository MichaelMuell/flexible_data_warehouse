/****** Object:  Table [dbo].[CLN_EKBE]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_EKBE](
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[AA_NUMBER] [decimal](2, 0) NULL,
	[MOVEMENT_TYPE] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
