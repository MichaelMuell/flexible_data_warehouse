/****** Object:  StoredProcedure [o2c].[p_tp1_exec]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp1_exec] as

exec o2c.p_tp10_customer

exec o2c.p_tp10_all_cust_items

exec o2c.p_tp10_dispute
GO
