/****** Object:  Table [o2c].[ing_bkpf]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_bkpf](
	[company_code] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[fiscal_year] [decimal](4, 0) NULL,
	[document_posted_by] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
