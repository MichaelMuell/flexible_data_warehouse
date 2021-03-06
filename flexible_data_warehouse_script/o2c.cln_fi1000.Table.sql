/****** Object:  Table [o2c].[cln_fi1000]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [o2c].[cln_fi1000](
	[company_code] [nvarchar](255) NULL,
	[customer_number] [nvarchar](255) NULL,
	[business_division] [nvarchar](255) NULL,
	[business_unit] [nvarchar](255) NULL,
	[credit_control_area] [nvarchar](255) NULL,
	[division] [nvarchar](255) NULL,
	[postdate] [date] NULL,
	[aramount] [float] NOT NULL,
	[salesamount] [float] NOT NULL,
	[overdueamount] [float] NOT NULL
) ON [PRIMARY]
GO
