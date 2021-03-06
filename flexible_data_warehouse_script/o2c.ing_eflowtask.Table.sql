/****** Object:  Table [o2c].[ing_eflowtask]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[ing_eflowtask](
	[task_id] [nvarchar](max) NULL,
	[processname] [nvarchar](max) NULL,
	[incident] [int] NULL,
	[steplabel] [nvarchar](max) NULL,
	[taskuser] [nvarchar](max) NULL,
	[assignedtouser] [nvarchar](max) NULL,
	[status] [int] NULL,
	[substatus] [int] NULL,
	[starttime] [datetime2](7) NULL,
	[endtime] [datetime2](7) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
