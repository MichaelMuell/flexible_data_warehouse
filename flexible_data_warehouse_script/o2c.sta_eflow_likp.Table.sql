/****** Object:  Table [o2c].[sta_eflow_likp]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_eflow_likp](
	[delivery_nr] [nvarchar](10) NOT NULL,
	[created_by] [nvarchar](40) NULL,
	[created_on] [datetime] NULL,
	[shippoint] [nvarchar](4) NULL,
	[salesorg] [nvarchar](4) NULL,
	[delivery_type] [nvarchar](4) NULL,
	[delivery_date] [datetime] NULL,
	[billing_block] [nvarchar](10) NULL,
	[soldtoparty] [nvarchar](10) NULL,
	[rel_cre_value] [decimal](15, 2) NULL,
	[rel_cre_date] [datetime] NULL,
	[actual_gi_date] [datetime] NULL,
	[file_path] [nvarchar](100) NULL,
	[download_date] [datetime] NULL,
	[src_download_date] [datetime] NULL,
	[durationmin_task_id] [int] NULL,
	[customer_country] [nvarchar](max) NULL,
	[trading_partner] [nvarchar](max) NULL,
	[customer_name1] [nvarchar](max) NULL,
	[customer_name2] [nvarchar](max) NULL,
	[delivery_status] [nvarchar](max) NULL,
	[gi_status] [nvarchar](max) NULL,
	[billing_status] [nvarchar](max) NULL,
	[status] [int] NULL,
	[statustext] [varchar](8) NULL,
	[substatus] [int] NULL,
	[task_id] [nvarchar](50) NULL,
	[durationmin] [int] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
