/****** Object:  User [sdp-s-p-fssc-df]    Script Date: 4/16/2021 11:35:26 AM ******/
CREATE USER [sdp-s-p-fssc-df] FROM  EXTERNAL PROVIDER  WITH DEFAULT_SCHEMA=[dbo]
GO
sys.sp_addrolemember @rolename = N'db_developer', @membername = N'sdp-s-p-fssc-df'
GO
sys.sp_addrolemember @rolename = N'db_datareader', @membername = N'sdp-s-p-fssc-df'
GO
sys.sp_addrolemember @rolename = N'db_datawriter', @membername = N'sdp-s-p-fssc-df'
GO
