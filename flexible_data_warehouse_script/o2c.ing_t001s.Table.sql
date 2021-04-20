/****** Object:  Table [o2c].[ing_t001s]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_t001s](
	[company_code] [nvarchar](max) NULL,
	[accounting_clerk_number] [nvarchar](max) NULL,
	[accounting_clerk_name] [nvarchar](max) NULL,
	[accounting_clerk_user] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
