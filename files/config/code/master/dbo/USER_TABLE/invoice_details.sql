CREATE TABLE [master].[dbo].[invoice_details]
(
    [invoice_detail_id] INT IDENTITY (1,1) NOT NULL,
    [invoice_id] INT NOT NULL,
    [product_id] INT NOT NULL
)
