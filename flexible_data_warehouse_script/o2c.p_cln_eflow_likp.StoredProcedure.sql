/****** Object:  StoredProcedure [o2c].[p_cln_eflow_likp]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_eflow_likp] AS

-- move content from ing data to cln table
truncate table o2c.cln_eflowtask
insert o2c.cln_eflowtask
select distinct 
	   left([task_id],50) [task_id]
	   ,left([processname],20) [processname]
      ,[incident]
      ,left([steplabel],30) [steplabel]
      ,left([taskuser],40) [taskuser]
      ,left([assignedtouser],40) [assignedtouser]
      ,[status]
      ,[substatus]
      ,[starttime]
      ,[endtime]
      ,[download_date]  
  from [o2c].[ing_eflowtask]
  where processname = 'P047_CLR_01' or ( processname = 'P048_GR_01' and steplabel = 'FI Release' )
        and endtime >='2020-01-01'

truncate table o2c.cln_eflowdn
insert o2c.cln_eflowdn
select distinct left(processname,20) processname,incident , left(dntbr,20) dntbr,   download_date from o2c.ing_eflowdn
 


GO
