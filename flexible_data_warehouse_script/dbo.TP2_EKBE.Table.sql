/****** Object:  Table [dbo].[TP2_EKBE]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP2_EKBE](
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[MATERIAL_DOCUMENT] [nvarchar](max) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](max) NULL,
	[POSTING_DATE] [date] NULL,
	[ENTRY_DATE] [date] NULL,
	[CREATED_BY] [nvarchar](max) NULL,
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[PLANT] [nvarchar](max) NULL,
	[QUANTITY] [decimal](13, 3) NULL,
	[PO_HISTORY_CATEGORY] [nvarchar](max) NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
