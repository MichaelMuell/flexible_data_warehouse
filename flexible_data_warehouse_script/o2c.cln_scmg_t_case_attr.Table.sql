/****** Object:  Table [o2c].[cln_scmg_t_case_attr]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_scmg_t_case_attr](
	[dispute_id] [nvarchar](max) NULL,
	[dispute_case_id] [nvarchar](max) NULL,
	[dispute_case_type] [nvarchar](max) NULL,
	[dispute_case_title] [nvarchar](max) NULL,
	[dispute_planned_close_date] [date] NULL,
	[dispute_reason] [nvarchar](max) NULL,
	[dispute_status] [decimal](2, 0) NULL,
	[dispute_responsible] [nvarchar](max) NULL,
	[dispute_processor] [nvarchar](max) NULL,
	[dispute_created_on] [decimal](15, 0) NULL,
	[dispute_closed_on] [decimal](15, 0) NULL,
	[dispute_changed_on] [decimal](15, 0) NULL,
	[file_path] [nvarchar](max) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
