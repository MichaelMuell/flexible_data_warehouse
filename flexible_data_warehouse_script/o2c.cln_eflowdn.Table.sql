/****** Object:  Table [o2c].[cln_eflowdn]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_eflowdn](
	[processname] [nvarchar](20) NOT NULL,
	[incident] [nvarchar](10) NOT NULL,
	[dntbr] [nvarchar](20) NULL,
	[download_date] [datetime] NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [inx_eflowdn]    Script Date: 4/16/2021 11:35:28 AM ******/
CREATE NONCLUSTERED INDEX [inx_eflowdn] ON [o2c].[cln_eflowdn]
(
	[processname] ASC,
	[incident] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO
