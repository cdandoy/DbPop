CREATE TABLE [master].[advanced].[orders]
(
    [order_id] INT IDENTITY (1,1) NOT NULL,
    [customer_id] INT NOT NULL,
    [order_type_id] INT,
    [order_date] DATETIME NOT NULL
)
