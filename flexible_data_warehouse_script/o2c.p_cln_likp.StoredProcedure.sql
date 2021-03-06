/****** Object:  StoredProcedure [o2c].[p_cln_likp]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2020/03/02
-- Description: data cleaning for delivery LIKP table
-- =============================================
CREATE PROCEDURE [o2c].[p_cln_likp]

AS
BEGIN

    insert o2c.cln_likp
	select  [delivery_nr],[created_by],	[created_on],[shippoint] ,[salesorg],
			[delivery_type], [delivery_date],[billing_block],[soldtoparty],	[rel_cre_value],
			[rel_cre_date],	[actual_gi_date], [file_path], [download_date], null
	 from o2c.ing_likp
	 where file_path in (select max(file_path) from o2c.ing_likp)
	

	if col_length ('o2c.cln_likp','durationmin_task_id') is null
	begin
	alter table o2c.cln_likp
	add durationmin_task_id int
	end

	update o2c.cln_likp
	 set o2c.cln_likp.src_download_date  = src.src_download_date
	 from o2c.cln_load_details as src
	 where table_name = 'LIKP'
END

GO
