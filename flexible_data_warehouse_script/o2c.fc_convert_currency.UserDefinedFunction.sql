/****** Object:  UserDefinedFunction [o2c].[fc_convert_currency]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE function [o2c].[fc_convert_currency]
(@curr1 nvarchar(max), @amount1 decimal(30,2), @curr2 nvarchar(max))
returns decimal(30,2)
as
begin

declare @amount2 decimal(30,2)

set @amount2 =  @amount1 / (select max(exchangerate) 
							from o2c.currency 
							where currency1 = @curr1 
									and 
								  currency2 = @curr2)

return @amount2
end
GO
