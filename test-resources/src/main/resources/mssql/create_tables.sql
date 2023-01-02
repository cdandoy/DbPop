CREATE TABLE master.dbo.customers
(
    customer_id INT PRIMARY KEY IDENTITY,
    name        VARCHAR(32)
);

CREATE TABLE master.dbo.products
(
    product_id INT PRIMARY KEY IDENTITY,
    part_no    VARCHAR(32)  NOT NULL,
    part_desc  VARCHAR(255) NOT NULL
);

CREATE TABLE master.dbo.invoices
(
    invoice_id   INT PRIMARY KEY IDENTITY,
    customer_id  INT      NOT NULL,
    invoice_date DATETIME NOT NULL,
    CONSTRAINT invoices_customers_fk FOREIGN KEY (customer_id) REFERENCES master.dbo.customers
);

CREATE TABLE master.dbo.invoice_details
(
    invoice_detail_id INT PRIMARY KEY IDENTITY,
    invoice_id        INT NOT NULL,
    product_id        INT NOT NULL,
    CONSTRAINT invoice_details_invoices_fk FOREIGN KEY (invoice_id) REFERENCES master.dbo.invoices,
    CONSTRAINT invoice_details_products_fk FOREIGN KEY (product_id) REFERENCES master.dbo.products
);

CREATE TABLE master.dbo.test_binary
(
    id          INT PRIMARY KEY IDENTITY,
    test_binary BINARY(11),
    test_blob   VARBINARY(MAX)
)