/****** Object:  User [RL_SDP_S_FSSC_DX_AAD]    Script Date: 4/16/2021 11:35:26 AM ******/
CREATE USER [RL_SDP_S_FSSC_DX_AAD] FROM  EXTERNAL PROVIDER 
GO
sys.sp_addrolemember @rolename = N'db_owner', @membername = N'RL_SDP_S_FSSC_DX_AAD'
GO
