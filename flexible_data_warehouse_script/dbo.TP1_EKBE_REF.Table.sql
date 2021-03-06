/****** Object:  Table [dbo].[TP1_EKBE_REF]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TP1_EKBE_REF](
	[MATERIAL_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_MAT_DOC] [decimal](4, 0) NULL,
	[PURCHASE_ORDER] [nvarchar](max) NULL,
	[REFERENCE_DOCUMENT] [nvarchar](10) NULL,
	[YEAR_REF_DOC] [decimal](4, 0) NULL,
	[REF_DOC_ENTRY_DATE] [date] NULL,
	[REF_DOC_POSTING_DATE] [date] NULL,
	[REF_DOC_CREATED_BY] [nvarchar](max) NULL,
	[GR_QUANTITY] [decimal](13, 3) NULL,
	[IR_QUANTITY] [decimal](13, 3) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
