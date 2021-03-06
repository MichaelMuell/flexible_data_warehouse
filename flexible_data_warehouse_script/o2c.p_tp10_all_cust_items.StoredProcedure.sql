/****** Object:  StoredProcedure [o2c].[p_tp10_all_cust_items]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp10_all_cust_items] as

drop table if exists o2c.tp1_all_cust_items

select 
	bsid.*, 
	bkpf.document_posted_by
	into o2c.tp1_all_cust_items 
	from o2c.cln_bsid as bsid
	left outer join o2c.cln_bkpf as bkpf on 
	bkpf.document_number = bsid.document_number and 
	bkpf.company_code = bsid.company_code and 
	bkpf.fiscal_year = bsid.fiscal_year
union
	select bsad.*, 
	bkpf.document_posted_by
	from o2c.cln_bsad as bsad
	left outer join o2c.cln_bkpf as bkpf on 
	bkpf.document_number = bsad.document_number and 
	bkpf.company_code = bsad.company_code and 
	bkpf.fiscal_year = bsad.fiscal_year;


-- because BSID and BSAD tables are downloaded at slightly different times there can be duplicates e.g. one item was open while bsid
-- was downlaoded an then cleared before bsad was downlaoded

with duplicates as (
    select *,
        row_number() over (
            partition by 
                transaction_key
            order by 
				transaction_key, 
				src_download_date desc
        ) row_num
     from 
		 o2c.tp1_all_cust_items
)

delete from duplicates
where row_num > 1
GO
