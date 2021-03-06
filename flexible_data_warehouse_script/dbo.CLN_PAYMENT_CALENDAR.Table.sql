/****** Object:  Table [dbo].[CLN_PAYMENT_CALENDAR]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CLN_PAYMENT_CALENDAR](
	[DATES] [nvarchar](max) NULL,
	[DOMESTIC_3RD_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_IC_PAYMENT] [nvarchar](max) NULL,
	[OVERSEA_3RD_PAYMENT] [nvarchar](max) NULL,
	[CHINA_PUBLIC_HOLIDAY] [nvarchar](max) NULL,
	[DOWNLOAD_DATE] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
