/****** Object:  Table [o2c].[cln_eflowtask]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_eflowtask](
	[task_id] [nvarchar](50) NOT NULL,
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[steplabel] [nvarchar](30) NULL,
	[taskuser] [nvarchar](40) NULL,
	[assignedtouser] [nvarchar](40) NULL,
	[status] [int] NULL,
	[substatus] [int] NULL,
	[starttime] [datetime] NULL,
	[endtime] [datetime] NULL,
	[download_date] [datetime] NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [inx_eflowtask]    Script Date: 4/16/2021 11:35:28 AM ******/
CREATE NONCLUSTERED INDEX [inx_eflowtask] ON [o2c].[cln_eflowtask]
(
	[processname] ASC,
	[incident] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
