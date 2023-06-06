CREATE TABLE [master].[dbo].[invoices]
(
    [invoice_id] INT IDENTITY (1,1) NOT NULL,
    [customer_id] INT NOT NULL,
    [invoice_date] DATETIME NOT NULL
)
