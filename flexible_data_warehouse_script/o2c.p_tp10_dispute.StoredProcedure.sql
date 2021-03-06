/****** Object:  StoredProcedure [o2c].[p_tp10_dispute]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [o2c].[p_tp10_dispute] as

drop table if exists o2c.tp1_dispute

select dc.*, 
udm.dispute_coordinator,
udm.dispute_process_deadline,
udm.dispute_detailed_cause,
scmg.dispute_case_id,
scmg.dispute_case_type,
scmg.dispute_case_title,
scmg.dispute_planned_close_date,
scmg.dispute_reason,
scmg.dispute_status,
scmg.dispute_responsible,
scmg.dispute_processor,
scmg.dispute_created_on,
scmg.dispute_closed_on,
scmg.dispute_changed_on
into o2c.tp1_dispute 
from o2c.cln_fdm_dcproc as dc 
	 left outer join 
	 o2c.cln_udmcaseattr00 as udm on
	 dc.dispute_id = udm.dispute_id 
	 left outer join 
	 o2c.cln_scmg_t_case_attr as scmg on 
	 dc.dispute_id = scmg.dispute_id
GO
