/****** Object:  StoredProcedure [o2c].[p_cln_adrc]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_adrc] as

update o2c.cln_adrc
set o2c.cln_adrc.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name <> 'adrc'

delete from o2c.cln_adrc
where file_path <> (select max(file_path) from o2c.cln_adrc)



GO
