CREATE TABLE [master].[advanced].[deliveries]
(
    [delivery_id] INT IDENTITY (1,1) NOT NULL,
    [invoice_detail_id] INT,
    [delivery_date] DATETIME NOT NULL
)
