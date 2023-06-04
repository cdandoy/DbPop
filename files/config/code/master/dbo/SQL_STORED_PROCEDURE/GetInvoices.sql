CREATE   PROCEDURE GetInvoices @invoiceId INT
AS
BEGIN
    SELECT *
    FROM master.dbo.invoices i
             JOIN invoice_details id ON i.invoice_id = id.invoice_id
             JOIN customers c ON i.customer_id = c.customer_id
             JOIN products p ON id.product_id = p.product_id
    WHERE i.invoice_id = @invoiceId
END