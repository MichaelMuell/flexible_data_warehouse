/****** Object:  Table [o2c].[ing_udmcaseattr00]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_udmcaseattr00](
	[dispute_id] [nvarchar](max) NULL,
	[dispute_coordinator] [nvarchar](max) NULL,
	[dispute_process_deadline] [date] NULL,
	[dispute_detailed_cause] [nvarchar](max) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
