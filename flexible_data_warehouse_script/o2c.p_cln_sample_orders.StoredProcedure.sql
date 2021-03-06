/****** Object:  StoredProcedure [o2c].[p_cln_sample_orders]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_sample_orders] AS



drop table if exists o2c.cln_sample_orders;
select * into o2c.cln_sample_orders from o2c.inx_sample_orders;

with overdue_duplicates as (
    select *,
        row_number() over (
            partition by 
                document_number,
				company_code, 
				[year], 
				line_item
            order by 
				document_number,
				company_code, 
				[year], 
				line_item, 
				key_date desc
        ) row_num
     from 
       o2c.cln_sample_orders
)

delete from overdue_duplicates
where row_num > 1 



exec o2c.p_execute_etl_function @imp_function = 'ADD_ZERO' , @imp_tablename = 'cln_sample_orders' ,@schema  = 'o2c'
GO
