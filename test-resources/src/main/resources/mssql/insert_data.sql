USE dbpop;

SET IDENTITY_INSERT customer_types ON;
INSERT INTO customer_types (customer_type_id, name)
VALUES (1, 'Wholesale'),
       (2, 'Retail');
SET IDENTITY_INSERT customer_types OFF;

SET IDENTITY_INSERT product_categories ON;
INSERT INTO product_categories (product_category_id, name)
VALUES (1, 'Electronic'),
       (2, 'Health');
SET IDENTITY_INSERT product_categories OFF;

SET IDENTITY_INSERT customers ON;
INSERT INTO customers (customer_id, customer_type_id, name)
VALUES (101, 1, 'AirMethod'),
       (102, 2, 'Crown Beauty Filters'),
       (103, 1, 'Salon Creatives');
SET IDENTITY_INSERT customers OFF;

SET IDENTITY_INSERT products ON;
INSERT INTO products (product_id, product_category_id, part_no, part_desc)
VALUES (11, 1, '3000', 'Circuit Playground Classic'),
       (12, 2, '4561', 'High Quality HQ Camera'),
       (13, 1, '5433', 'Pogo Pin Probe Clip');
SET IDENTITY_INSERT products OFF;

SET IDENTITY_INSERT invoices ON;
INSERT INTO invoices (invoice_id, customer_id, invoice_date)
VALUES (1001, 101, '2022-01-01'),
       (1002, 102, '2022-01-01'),
       (1003, 101, '2022-01-01'),
       (1004, 102, '2022-01-01');
SET IDENTITY_INSERT invoices OFF;

SET IDENTITY_INSERT invoice_details ON;
INSERT INTO invoice_details (invoice_detail_id, invoice_id, product_id)
VALUES (10001, 1001, 11),
       (10002, 1001, 12),
       (10003, 1002, 12),
       (10004, 1002, 13),
       (10005, 1003, 11),
       (10006, 1003, 12),
       (10007, 1003, 13);
SET IDENTITY_INSERT invoice_details OFF;
GO
SET IDENTITY_INSERT test_binary ON;
INSERT INTO test_binary (id, test_binary, test_blob)
VALUES (1,
        CAST(('HELLO' + CAST(0x00 AS CHAR(1)) + 'WORLD') AS BINARY(11)),
        CAST('HELLO' + CAST(0x00 AS CHAR(1)) + 'WORLD' AS VARBINARY(MAX)));
SET IDENTITY_INSERT test_binary OFF;
