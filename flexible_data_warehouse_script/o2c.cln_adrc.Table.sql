/****** Object:  Table [o2c].[cln_adrc]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_adrc](
	[address_number] [nvarchar](max) NULL,
	[nation] [nvarchar](max) NULL,
	[name1] [nvarchar](max) NULL,
	[name2] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
