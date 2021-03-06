/****** Object:  Table [o2c].[cln_t052]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_t052](
	[payment_term] [nvarchar](max) NULL,
	[day_limit] [decimal](2, 0) NULL,
	[date_type] [nvarchar](max) NULL,
	[calendar_day_for_baseline_date_of_payment] [decimal](2, 0) NULL,
	[additional_months] [decimal](2, 0) NULL,
	[days1_fixed] [decimal](3, 0) NULL,
	[days2_fixed] [decimal](3, 0) NULL,
	[days3_fixed] [decimal](3, 0) NULL,
	[due_date_special1] [decimal](2, 0) NULL,
	[month_special1] [decimal](2, 0) NULL,
	[file_path] [nvarchar](max) NULL,
	[due_date_special2] [decimal](2, 0) NULL,
	[month_special2] [decimal](2, 0) NULL,
	[due_date_special3] [decimal](2, 0) NULL,
	[month_special3] [decimal](2, 0) NULL,
	[download_date] [datetime2](7) NULL,
	[src_download_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
