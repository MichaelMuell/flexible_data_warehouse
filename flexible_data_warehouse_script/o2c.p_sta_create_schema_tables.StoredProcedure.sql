/****** Object:  StoredProcedure [o2c].[p_sta_create_schema_tables]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_sta_create_schema_tables] as 

drop table if exists o2c.sta_all_cust_items_schema
select top(500)* into o2c.sta_all_cust_items_schema from o2c.sta_all_cust_items
where reference_key1 <> ''

drop table if exists o2c.sta_open_cust_items_schema
select top(500)* into o2c.sta_open_cust_items_schema from o2c.sta_open_cust_items
where reference_key1 <> ''

drop table if exists o2c.sta_eflow_clr_schema
select top(500)* into o2c.sta_eflow_clr_schema from o2c.sta_eflow_clr

drop table if exists o2c.sta_eflow_likp_schema
select top(500)* into o2c.sta_eflow_likp_schema from o2c.sta_eflow_likp

drop table if exists o2c.sta_fi1000_schema
select top(500)* into o2c.sta_fi1000_schema from o2c.sta_fi1000

drop table if exists o2c.sta_payment_behavior_schema
select top(500)* into o2c.sta_payment_behavior_schema from o2c.sta_payment_behavior
GO
