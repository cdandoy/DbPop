USE master;

DROP TABLE IF EXISTS invoice_details;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers
(
    customer_id INT PRIMARY KEY IDENTITY,
    name        VARCHAR(32)
);

CREATE TABLE products
(
    product_id INT PRIMARY KEY IDENTITY,
    part_no    VARCHAR(32)  NOT NULL,
    part_desc  VARCHAR(255) NOT NULL
);

CREATE TABLE invoices
(
    invoice_id   INT PRIMARY KEY IDENTITY,
    customer_id  INT      NOT NULL REFERENCES customers,
    invoice_date DATETIME NOT NULL
);

CREATE TABLE invoice_details
(
    invoice_detail_id INT PRIMARY KEY IDENTITY,
    invoice_id        INT NOT NULL REFERENCES invoices,
    product_id        INT NOT NULL REFERENCES products
);