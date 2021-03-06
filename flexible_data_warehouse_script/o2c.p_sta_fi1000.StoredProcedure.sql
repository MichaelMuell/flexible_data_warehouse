/****** Object:  StoredProcedure [o2c].[p_sta_fi1000]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      wangynh
-- Create Date: 2021/03/12
-- Description: combine bwfi1000 report and customer master data 
-- =============================================
CREATE PROCEDURE [o2c].[p_sta_fi1000]
AS
BEGIN

	drop  table if exists o2c.sta_fi1000;

	select * 
	into o2c.sta_fi1000
	from o2c.tp3_fi1000

	exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_fi1000', @schema = 'o2c'

end
GO
