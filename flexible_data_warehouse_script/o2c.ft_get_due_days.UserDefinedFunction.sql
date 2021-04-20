/****** Object:  UserDefinedFunction [o2c].[ft_get_due_days]    Script Date: 4/16/2021 11:35:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [o2c].[ft_get_due_days]
(
@transaction_key varchar(max) 
) 
returns @ResultTable table
( 
days1 varchar(max), days2 varchar(max)
) AS BEGIN

        INSERT INTO @ResultTable (days1, days2)
		select currency,key_date 
		from o2c.tp3_all_cust_items where 
		transaction_key = @transaction_key
               
RETURN
END

GO
