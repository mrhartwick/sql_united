USE [master]
GO

/****** Object:  LinkedServer [10.2.186.148,4721]    Script Date: 5/17/2018 4:06:40 PM ******/
EXEC master.dbo.sp_addlinkedserver @server = N'10.2.186.148,4721', @srvproduct=N'', @provider=N'SQLNCLI', @datasrc=N'10.2.186.148,4721,4721'
 /* For security reasons the linked server remote logins password is changed with ######## */
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'10.2.186.148,4721',@useself=N'False',@locallogin=NULL,@rmtuser=N'mhartwick',@rmtpassword='KpfbTKzZ7Dmh'

GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'collation compatible', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'data access', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'dist', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'pub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'rpc', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'rpc out', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'sub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'connect timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'collation name', @optvalue=null
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'lazy schema validation', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'query timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'use remote collation', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'10.2.186.148,4721', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO
