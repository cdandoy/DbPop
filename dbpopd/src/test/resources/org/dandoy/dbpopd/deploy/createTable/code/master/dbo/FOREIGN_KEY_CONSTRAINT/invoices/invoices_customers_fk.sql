ALTER TABLE [master].[dbo].[invoices] ADD CONSTRAINT [invoices_customers_fk] FOREIGN KEY ([customer_id]) REFERENCES [master].[dbo].[customers] ([customer_id])