/****** Object:  StoredProcedure [o2c].[p_tp2_exec]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp2_exec] as 

exec o2c.p_tp20_all_cust_items

exec o2c.p_tp20_fi1000
GO
