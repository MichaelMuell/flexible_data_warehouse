/****** Object:  Table [dbo].[ING_BSAK]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_BSAK](
	[CLIENT] [nvarchar](max) NULL,
	[COMPANY_CODE] [nvarchar](max) NULL,
	[DOCUMENT_NUMBER] [nvarchar](max) NULL,
	[LINE_ITEM] [decimal](3, 0) NULL,
	[VENDOR_NUMBER] [nvarchar](max) NULL,
	[DOCUMENT_TYPE] [nvarchar](max) NULL,
	[SPECIAL_GL_INDICATOR] [nvarchar](max) NULL,
	[PAYMENT_BLOCK] [nvarchar](max) NULL,
	[PAYMENT_TERMS] [nvarchar](max) NULL,
	[SCB_INDICATOR] [nvarchar](max) NULL,
	[GL_ACCOUNT] [nvarchar](max) NULL,
	[CLEARING_DOCUMENT] [nvarchar](max) NULL,
	[CURRENCY] [nvarchar](max) NULL,
	[POSTING_DATE] [nvarchar](max) NULL,
	[CLEARING_DATE] [nvarchar](max) NULL,
	[AMOUNT_LOCAL] [decimal](13, 2) NULL,
	[AMOUNT_DOCUMENT] [decimal](13, 2) NULL,
	[REFERENCE] [nvarchar](max) NULL,
	[ITEM_TEXT] [nvarchar](max) NULL,
	[YEAR] [decimal](4, 0) NULL,
	[DAYS1] [decimal](3, 0) NULL,
	[DAYS2] [decimal](3, 0) NULL,
	[BASELINE_DATE] [nvarchar](max) NULL,
	[FILE_PATH] [nvarchar](max) NULL,
	[DEBIT_CREDIT] [nvarchar](max) NULL,
	[POSTING_KEY] [nvarchar](max) NULL,
	[FOLLOW_ON_DOC] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
