/****** Object:  StoredProcedure [o2c].[p_sta_exec]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_exec] as 

exec o2c.p_sta_open_cust_items 

exec o2c.p_sta_all_cust_items 

exec o2c.p_sta_eflow_clr 

exec o2c.p_sta_eflow_likp

exec o2c.p_sta_fi1000

exec o2c.p_sta_payment_behavior

exec o2c.p_sta_create_schema_tables
GO
