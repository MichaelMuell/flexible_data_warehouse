/****** Object:  StoredProcedure [o2c].[p_cln_vbuk]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2020/03/02
-- Description: <clean table vbuk
-- =============================================
CREATE PROCEDURE [o2c].[p_cln_vbuk]
AS
BEGIN
--recreate new cleaning VBUK table with index
	drop table if exists o2c.cln_vbuk 
	create table [o2c].[cln_vbuk](
		[delivery_nr] [nvarchar](10) not null primary key,
		[delivery_status] [nvarchar](1) null,
		[gi_status] [nvarchar](1) null,
		[billing_status] [nvarchar](1) null,
		[file_path] [nvarchar](100),
		[download_date] [datetime] null,
		[src_download_date] [nvarchar](max) null
	) 

	insert o2c.cln_vbuk 
	select  [delivery_nr],[delivery_status],[gi_status],[billing_status],[file_path], [download_date], null from o2c.ing_vbuk
	 where file_path in (select max(file_path) from o2c.ing_vbuk)

	update o2c.cln_vbuk
set o2c.cln_vbuk.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'VBUK'
	END
GO
