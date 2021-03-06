/****** Object:  StoredProcedure [o2c].[p_cln_exec]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [o2c].[p_cln_exec] @schema varchar(max) AS

-- FIRST---------------------------------------------------------------------

exec o2c.p_cln_first @schema = 'o2c'

exec o2c.p_cln_src_download_date @schema = 'o2c'

exec o2c.p_cln_load_details
------

exec o2c.p_cln_adrc

exec o2c.p_cln_bkpf

exec o2c.p_cln_bsad

exec o2c.p_cln_bsid

exec o2c.p_cln_fdm_dcproc

exec o2c.p_cln_kna1

exec o2c.p_cln_knb1

exec o2c.p_cln_knkk

exec o2c.p_cln_scmg_t_case_attr

exec o2c.p_cln_t001

exec o2c.p_cln_t001s

exec o2c.p_cln_t052

exec o2c.p_cln_udmcaseattr00

exec o2c.p_cln_eflow_likp

exec o2c.p_cln_likp

exec o2c.p_cln_vbuk

exec o2c.p_cln_fi1000

exec o2c.p_cln_sample_orders

--LAST   ---------------------------------------------------------------------------

exec o2c.p_cln_clean_columns @schema = 'o2c'
GO
