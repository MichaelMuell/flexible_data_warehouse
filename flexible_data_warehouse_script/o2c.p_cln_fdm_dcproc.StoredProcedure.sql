/****** Object:  StoredProcedure [o2c].[p_cln_fdm_dcproc]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_fdm_dcproc] as

delete from o2c.cln_fdm_dcproc
where file_path <> (select max(file_path) from o2c.cln_fdm_dcproc);

with duplicates as (
    select *,
        row_number() over (
            partition by 
                transaction_key
		    order by 
		        transaction_key
        ) row_num
     from 
        o2c.cln_fdm_dcproc
)

delete from duplicates
where row_num > 1

update o2c.cln_fdm_dcproc
set o2c.cln_fdm_dcproc.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 'FDM_DCPROC'
GO
