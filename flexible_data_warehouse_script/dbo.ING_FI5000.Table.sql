/****** Object:  Table [dbo].[ING_FI5000]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ING_FI5000](
	[ACTIVITY_STATUS] [nvarchar](255) NULL,
	[YEAR] [nvarchar](255) NULL,
	[CASH_DISCOUNT2] [nvarchar](50) NULL,
	[CLEARING_DATE] [nvarchar](50) NULL,
	[COMPANY_CODE] [nvarchar](255) NULL,
	[CURRENCY] [nvarchar](255) NULL,
	[ENTERED_ON_DATE] [nvarchar](255) NULL,
	[FI_DOCUMENT_NO] [nvarchar](255) NULL,
	[INPUT_DATE] [nvarchar](50) NULL,
	[INVOICE_DATE] [nvarchar](50) NULL,
	[INVOICE_INPUT_CHANNEL] [nvarchar](255) NULL,
	[INVOICE_STATE] [nvarchar](255) NULL,
	[LEGAL_ENTITY] [nvarchar](255) NULL,
	[OCR_INVOICE_CORRECTION] [nvarchar](255) NULL,
	[OCR_STACK_NAME] [nvarchar](255) NULL,
	[OCR_SUPPLIER_CORRECTION] [nvarchar](255) NULL,
	[PAYING_DATE] [nvarchar](50) NULL,
	[POSTING_DATE] [nvarchar](50) NULL,
	[PURCHASE_ORDER] [nvarchar](255) NULL,
	[REFERENCE] [nvarchar](255) NULL,
	[SCANTIME] [nvarchar](255) NULL,
	[SOURCE_SYSTEM] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING] [nvarchar](255) NULL,
	[VENDOR_NUMBER] [nvarchar](255) NULL,
	[TEAM_HIST] [nvarchar](255) NULL,
	[TRANSACTION_KEY] [nvarchar](255) NOT NULL,
	[AMOUNT_DOCUMENT] [float] NULL,
	[AMOUNT_LOCAL] [float] NULL,
	[TAX_AMOUNT] [float] NULL,
	[AUTH_LEGAL_REGION] [nvarchar](255) NULL,
	[ACTIVITY_STATUS_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_STATE_DESCRIPTION] [nvarchar](255) NULL,
	[CURRENCY_DESCRIPTION] [nvarchar](255) NULL,
	[SUPPLIER_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[STATE_AUTO_POSTING_DESCRIPTION] [nvarchar](255) NULL,
	[INVOICE_CORRECTION_DESCRIPTION] [nvarchar](255) NULL,
	[SCAN_DATE] [nvarchar](50) NULL,
	[DELIVERY_NOTE] [nvarchar](50) NULL,
	[MAT_DOCUMENT_NO] [nvarchar](50) NULL,
	[SRC_DOWNLOAD_DATE] [datetime] NULL
) ON [PRIMARY]
GO
