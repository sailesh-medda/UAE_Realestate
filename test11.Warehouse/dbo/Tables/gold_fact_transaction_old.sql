CREATE TABLE [dbo].[gold_fact_transaction_old] (

	[transaction_id] varchar(8000) NULL, 
	[instance_date] date NULL, 
	[procedure_area] int NULL, 
	[actual_worth] int NULL, 
	[meter_sale_price] int NULL, 
	[rent_value] int NULL, 
	[meter_rent_price] int NULL, 
	[dim_type_key] bigint NULL, 
	[dim_area_key] bigint NULL
);

