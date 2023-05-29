DROP PROCEDURE IF EXISTS GetInvoices
GO
DROP TABLE IF EXISTS master.dbo.invoice_details
GO
DROP TABLE IF EXISTS master.dbo.invoices
GO
DROP TABLE IF EXISTS master.dbo.products
GO
DROP TABLE IF EXISTS master.dbo.customers
GO
DROP TABLE IF EXISTS master.dbo.test_binary
GO

CREATE TABLE master.dbo.customers
(
    customer_id INT PRIMARY KEY IDENTITY,
    name        VARCHAR(32)
)
GO

CREATE TABLE master.dbo.products
(
    product_id INT PRIMARY KEY IDENTITY,
    part_no    VARCHAR(32)  NOT NULL,
    part_desc  VARCHAR(255) NOT NULL
)
GO

CREATE TABLE master.dbo.invoices
(
    invoice_id   INT PRIMARY KEY IDENTITY,
    customer_id  INT      NOT NULL,
    invoice_date DATETIME NOT NULL,
    CONSTRAINT invoices_customers_fk FOREIGN KEY (customer_id) REFERENCES master.dbo.customers
)
GO

CREATE TABLE master.dbo.invoice_details
(
    invoice_detail_id INT PRIMARY KEY IDENTITY,
    invoice_id        INT NOT NULL,
    product_id        INT NOT NULL,
    CONSTRAINT invoice_details_invoices_fk FOREIGN KEY (invoice_id) REFERENCES master.dbo.invoices,
    CONSTRAINT invoice_details_products_fk FOREIGN KEY (product_id) REFERENCES master.dbo.products
)
GO

CREATE TABLE master.dbo.test_binary
(
    id          INT PRIMARY KEY IDENTITY,
    test_binary BINARY(11),
    test_blob   VARBINARY(MAX)
)
GO

USE master
GO

IF SCHEMA_ID('advanced') IS NULL
    EXEC ('create schema advanced')

DROP TABLE IF EXISTS master.advanced.order_details
GO
DROP TABLE IF EXISTS master.advanced.orders
GO
DROP TABLE IF EXISTS master.advanced.deliveries
GO
DROP TABLE IF EXISTS master.advanced.invoice_details
GO
DROP TABLE IF EXISTS master.advanced.invoices
GO
DROP TABLE IF EXISTS master.advanced.products
GO
DROP TABLE IF EXISTS master.advanced.customers
GO
DROP TABLE IF EXISTS master.advanced.order_types
GO
DROP TABLE IF EXISTS master.advanced.product_categories
GO
DROP TABLE IF EXISTS master.advanced.customer_types
GO

CREATE TABLE master.advanced.customer_types
(
    customer_type_id INT PRIMARY KEY IDENTITY,
    name             VARCHAR(32)
)
GO

CREATE TABLE master.advanced.product_categories
(
    product_category_id INT PRIMARY KEY IDENTITY,
    name                VARCHAR(32)
)
GO

CREATE TABLE master.advanced.order_types
(
    order_type_id INT PRIMARY KEY IDENTITY,
    name          VARCHAR(32)
)
GO

CREATE TABLE master.advanced.customers
(
    customer_id      INT PRIMARY KEY IDENTITY,
    customer_type_id INT,
    name             VARCHAR(32),
    CONSTRAINT customers_customer_types_fk FOREIGN KEY (customer_type_id) REFERENCES master.advanced.customer_types
)
GO

CREATE TABLE master.advanced.products
(
    product_id          INT PRIMARY KEY IDENTITY,
    product_category_id INT,
    part_no             VARCHAR(32)  NOT NULL,
    part_desc           VARCHAR(255) NOT NULL,
    CONSTRAINT products_product_categories_fk FOREIGN KEY (product_category_id) REFERENCES master.advanced.product_categories
)
GO

CREATE TABLE master.advanced.invoices
(
    invoice_id   INT PRIMARY KEY IDENTITY,
    customer_id  INT      NOT NULL,
    invoice_date DATETIME NOT NULL,
    CONSTRAINT invoices_customers_fk FOREIGN KEY (customer_id) REFERENCES master.advanced.customers
)
GO

CREATE TABLE master.advanced.invoice_details
(
    invoice_detail_id INT PRIMARY KEY IDENTITY,
    invoice_id        INT NOT NULL,
    product_id        INT NOT NULL,
    CONSTRAINT invoice_details_invoices_fk FOREIGN KEY (invoice_id) REFERENCES master.advanced.invoices,
    CONSTRAINT invoice_details_products_fk FOREIGN KEY (product_id) REFERENCES master.advanced.products
)
GO

CREATE TABLE master.advanced.deliveries
(
    delivery_id       INT PRIMARY KEY IDENTITY,
    invoice_detail_id INT,
    delivery_date     DATETIME NOT NULL,
    CONSTRAINT deliveries_invoice_details_fk FOREIGN KEY (invoice_detail_id) REFERENCES master.advanced.invoice_details
)
GO

CREATE TABLE master.advanced.orders
(
    order_id      INT PRIMARY KEY IDENTITY,
    customer_id   INT      NOT NULL,
    order_type_id INT,
    order_date    DATETIME NOT NULL,
    CONSTRAINT orders_customers_fk FOREIGN KEY (customer_id) REFERENCES master.advanced.customers,
    CONSTRAINT orders_order_types_fk FOREIGN KEY (order_type_id) REFERENCES master.advanced.order_types
)
GO

CREATE TABLE master.advanced.order_details
(
    order_detail_id INT PRIMARY KEY IDENTITY,
    order_id        INT NOT NULL,
    product_id      INT NOT NULL,
    CONSTRAINT order_details_orders_fk FOREIGN KEY (order_id) REFERENCES master.advanced.orders,
    CONSTRAINT order_details_products_fk FOREIGN KEY (product_id) REFERENCES master.advanced.products
)
GO
CREATE OR ALTER PROCEDURE GetInvoices @invoiceId INT
AS
BEGIN
    SELECT *
    FROM master.dbo.invoices i
             JOIN invoice_details id ON i.invoice_id = id.invoice_id
             JOIN customers c ON i.customer_id = c.customer_id
             JOIN products p ON id.product_id = p.product_id
    WHERE i.invoice_id = @invoiceId
END
