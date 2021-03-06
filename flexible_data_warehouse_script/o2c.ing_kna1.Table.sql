/****** Object:  Table [o2c].[ing_kna1]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_kna1](
	[customer_number] [nvarchar](max) NULL,
	[customer_country] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[address_number] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[account_group] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
