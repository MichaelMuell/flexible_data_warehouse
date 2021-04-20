/****** Object:  StoredProcedure [o2c].[p_cln_load_details]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [o2c].[p_cln_load_details] as

delete from o2c.cln_load_details
where file_path <> (select max(file_path) from o2c.cln_load_details)

update o2c.cln_load_details set table_name = substring(table_name,5,len(table_name))
GO
