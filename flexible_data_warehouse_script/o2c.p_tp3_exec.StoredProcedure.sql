/****** Object:  StoredProcedure [o2c].[p_tp3_exec]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp3_exec] as 

exec o2c.p_tp30_first

exec o2c.p_tp30_all_cust_items 
GO
