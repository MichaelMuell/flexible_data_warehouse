/****** Object:  StoredProcedure [o2c].[p_sta_eflow_likp]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_sta_eflow_likp] AS

-- create temperary table for transforming...

drop table if exists o2c.#cln_dnsum
select  a.PROCESSNAME,a.INCIDENT,right(concat('00000',DNTBR),10) as DELIVERY_NR ,
b.task_id, b.STEPLABEL, b.status, b.substatus, 
	   case
	     when b.status = 1 then 'Open'
		 when b.status = 3 then 'Complete'
		 when b.status = 4 then 'Return'
		 when b.status = 7 then 'Rejected'
	   end as StatusText,
	   b.starttime, b.endtime,
	   case 
	    when b.status = 7 then 0
		else  o2c.fc_cal_eflow_duration(b.starttime,b.endtime)  
	   end as  'DURATIONMIN' 
into o2c.#cln_dnsum
from o2c.cln_eflowdn as a 
left outer join o2c.cln_eflowtask  as b 
on a.PROCESSNAME = b.PROCESSNAME and a.INCIDENT = b.INCIDENT 
where b.processname = 'P048_GR_01' and b.endtime >='2020-01-01'

-- delete temperary table 


delete from o2c.#cln_dnsum where task_id is null ;

with dn_duplicates as (
    select *,
        row_number() over (
            partition by 
                delivery_nr
		    order by 
		        delivery_nr,
				convert(datetime,starttime) desc
        ) row_num
     from 
        o2c.#cln_dnsum
)
delete from dn_duplicates
where row_num > 1

drop table if exists o2c.sta_eflow_likp
select a.*,
	   d.customer_country,
	   d.trading_partner,
	   d.customer_name1,
	   d.customer_name2,
       c.delivery_status, 
	   c.gi_status, 
	   c.billing_status,
	   b.status,
	   b.statustext,
	   b.substatus,
	   b.task_id,
	isnull(durationmin,-1) durationmin
into o2c.sta_eflow_likp
from o2c.cln_likp a
left outer join o2c.#cln_dnsum b
on a.delivery_nr = b.delivery_nr  and 
convert(date,a.rel_cre_date) = convert(date,b.starttime)
left outer join o2c.ing_vbuk c
on a.delivery_nr = c.delivery_nr
left outer join o2c.cln_kna1 d 
 on a.SOLDTOPARTY = d.customer_number
order by durationmin desc

drop table if exists o2c.#cln_dnsum

exec o2c.p_execute_etl_function @imp_function = 'REMOVE_ZERO', @imp_tablename = 'sta_eflow_likp', @schema = 'o2c';

WITH measure_based_on_task_id AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                task_id
            ORDER BY 
				task_id
        ) row_num
     FROM 
        o2c.sta_eflow_likp
)

update o2c.sta_eflow_likp set o2c.sta_eflow_likp.durationmin_task_id = measure_based_on_task_id.durationmin
from measure_based_on_task_id
where measure_based_on_task_id.delivery_nr = o2c.sta_eflow_likp.delivery_nr and 
row_num = 1

GO
