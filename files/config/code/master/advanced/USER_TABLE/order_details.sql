CREATE TABLE [master].[advanced].[order_details]
(
    [order_detail_id] INT IDENTITY (1,1) NOT NULL,
    [order_id] INT NOT NULL,
    [product_id] INT NOT NULL
)
