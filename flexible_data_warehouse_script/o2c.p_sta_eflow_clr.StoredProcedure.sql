/****** Object:  StoredProcedure [o2c].[p_sta_eflow_clr]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [o2c].[p_sta_eflow_clr]
AS
BEGIN
--Statisit table eflowclr old version , dont consider holiday 
/**
	drop table if exists o2c.sta_eflow_clr
	select processname,incident,steplabel,status, 
	  case 
	     when status = 1 then 'open'
		 when status =3 then 'Complete'
		 when status = 4 then 'Return'
		 when status = 7 then 'Rejected'
	  end as StatusText,
	  substatus ,taskuser, assignedtouser,starttime,endtime,task_id,
	  DATEDIFF(minute,STARTTIME,ENDTIME) as 'durationmin' 
	  into o2c.sta_eflow_clr
	 from o2c.cln_eflowtask
	 where processname = 'P047_CLR_01'   
	   and endtime >='2020-01-01'
**/

-- New version consider the payment calendar
  drop table if exists o2c.sta_eflow_clr
	select processname,incident,steplabel,status, 
	  case 
	     when status = 1 then 'open'
		 when status =3 then 'Complete'
		 when status = 4 then 'Return'
		 when status = 7 then 'Rejected'
	  end as StatusText,
	  substatus ,taskuser, assignedtouser,starttime,endtime,task_id,
	  o2c.fc_cal_eflow_duration(starttime,endtime) as 'durationmin' 
	  into o2c.sta_eflow_clr
	 from o2c.cln_eflowtask
	 where processname = 'P047_CLR_01'   
	   and endtime >='2020-01-01'

END
GO
