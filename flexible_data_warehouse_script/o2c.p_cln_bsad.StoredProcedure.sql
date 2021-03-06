/****** Object:  StoredProcedure [o2c].[p_cln_bsad]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_bsad] as

delete from o2c.cln_bsad
where file_path <> (select max(file_path) from o2c.cln_bsad)

update o2c.cln_bsad
set o2c.cln_bsad.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'BSAD'
GO
