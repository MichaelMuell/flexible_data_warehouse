/****** Object:  Table [o2c].[sta_eflow_clr_schema]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[sta_eflow_clr_schema](
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[steplabel] [nvarchar](30) NULL,
	[status] [int] NULL,
	[StatusText] [varchar](8) NULL,
	[substatus] [int] NULL,
	[taskuser] [nvarchar](40) NULL,
	[assignedtouser] [nvarchar](40) NULL,
	[starttime] [datetime] NULL,
	[endtime] [datetime] NULL,
	[task_id] [nvarchar](50) NOT NULL,
	[durationmin] [int] NULL
) ON [PRIMARY]
GO
