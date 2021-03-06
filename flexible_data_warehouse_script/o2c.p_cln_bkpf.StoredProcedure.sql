/****** Object:  StoredProcedure [o2c].[p_cln_bkpf]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_bkpf] as

delete from o2c.cln_bkpf
where file_path <> (select max(file_path) from o2c.cln_bkpf)

update o2c.cln_bkpf
set o2c.cln_bkpf.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'BKPF_FSSC_improvement_Framework'
GO
