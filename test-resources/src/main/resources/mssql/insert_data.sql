USE master;

SET IDENTITY_INSERT master.dbo.customers ON;
INSERT INTO master.dbo.customers (customer_id, name)
VALUES (101, N'AirMethod'),
       (102, N'Crown Beauty Filters'),
       (103, N'Salon Creatives');
SET IDENTITY_INSERT master.dbo.customers OFF;

SET IDENTITY_INSERT master.dbo.products ON;
INSERT INTO master.dbo.products (product_id, part_no, part_desc)
VALUES (11, '3000', 'Circuit Playground Classic'),
       (12, '4561', 'High Quality HQ Camera'),
       (13, '5433', 'Pogo Pin Probe Clip');
SET IDENTITY_INSERT master.dbo.products OFF;

SET IDENTITY_INSERT master.dbo.invoices ON;
INSERT INTO master.dbo.invoices (invoice_id, customer_id, invoice_date)
VALUES (1001, 101, '2022-01-01'),
       (1002, 102, '2022-01-01'),
       (1003, 101, '2022-01-01'),
       (1004, 102, '2022-01-01');
SET IDENTITY_INSERT master.dbo.invoices OFF;

SET IDENTITY_INSERT master.dbo.invoice_details ON;
INSERT INTO master.dbo.invoice_details (invoice_detail_id, invoice_id, product_id)
VALUES (10001, 1001, 11),
       (10002, 1001, 12),
       (10003, 1002, 12),
       (10004, 1002, 13),
       (10005, 1003, 11),
       (10006, 1003, 12),
       (10007, 1003, 13);
SET IDENTITY_INSERT master.dbo.invoice_details OFF;

SET IDENTITY_INSERT master.dbo.test_binary ON;
INSERT INTO master.dbo.test_binary (id, test_binary, test_blob)
VALUES (1,
        CAST(('HELLO' + CAST(0x00 AS CHAR(1)) + 'WORLD') AS BINARY(11)),
        CAST('HELLO' + CAST(0x00 AS CHAR(1)) + 'WORLD' AS VARBINARY(MAX)));
SET IDENTITY_INSERT master.dbo.test_binary OFF;
