/****** Object:  Table [o2c].[cln_sample_orders]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_sample_orders](
	[document_number] [nvarchar](max) NULL,
	[company_code] [nvarchar](max) NULL,
	[year] [nvarchar](max) NULL,
	[line_item] [nvarchar](max) NULL,
	[key_date] [nvarchar](max) NULL,
	[sample_order] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
