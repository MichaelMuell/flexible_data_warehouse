/****** Object:  StoredProcedure [o2c].[p_cln_knkk]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_knkk] as

delete from o2c.cln_knkk
where file_path <> (select max(file_path) from o2c.cln_knkk)

update o2c.cln_knkk
set o2c.cln_knkk.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'KNKK'
GO
