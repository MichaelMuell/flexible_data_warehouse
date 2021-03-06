/****** Object:  Table [o2c].[cln_likp]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_likp](
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
PRIMARY KEY CLUSTERED 
(
	[delivery_nr] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
