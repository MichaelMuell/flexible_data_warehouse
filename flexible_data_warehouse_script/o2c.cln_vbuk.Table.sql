/****** Object:  Table [o2c].[cln_vbuk]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_vbuk](
	[delivery_nr] [nvarchar](10) NOT NULL,
	[delivery_status] [nvarchar](1) NULL,
	[gi_status] [nvarchar](1) NULL,
	[billing_status] [nvarchar](1) NULL,
	[file_path] [nvarchar](100) NULL,
	[download_date] [datetime] NULL,
	[src_download_date] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[delivery_nr] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
