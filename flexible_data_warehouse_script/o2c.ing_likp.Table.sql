/****** Object:  Table [o2c].[ing_likp]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_likp](
	[delivery_nr] [nvarchar](max) NULL,
	[created_by] [nvarchar](max) NULL,
	[created_on] [datetime2](7) NULL,
	[shippoint] [nvarchar](max) NULL,
	[salesorg] [nvarchar](max) NULL,
	[delivery_type] [nvarchar](max) NULL,
	[delivery_date] [datetime2](7) NULL,
	[billing_block] [nvarchar](max) NULL,
	[soldtoparty] [nvarchar](max) NULL,
	[rel_cre_value] [decimal](15, 2) NULL,
	[rel_cre_date] [datetime2](7) NULL,
	[actual_gi_date] [datetime2](7) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
