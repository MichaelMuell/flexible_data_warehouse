/****** Object:  StoredProcedure [o2c].[p_cln_t001s]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_t001s] as

delete from o2c.cln_t001s
where file_path <> (select max(file_path) from o2c.cln_t001s);

with t001s_duplicates as (
    select *,
        row_number() over (
            partition by 
                company_code, 
                accounting_clerk_number
		    order by 
		        company_code,
				 accounting_clerk_number
        ) row_num
     from 
        o2c.cln_t001s
)

delete from t001s_duplicates
where row_num > 1

update o2c.cln_t001s
set o2c.cln_t001s.src_download_date  = src.src_download_date
from o2c.cln_load_details as src
where table_name = 't001s'
GO
