USE dbpop;
GO
CREATE TABLE customer_types
(
    customer_type_id INT PRIMARY KEY IDENTITY,
    name             VARCHAR(32)
);

CREATE TABLE product_categories
(
    product_category_id INT PRIMARY KEY IDENTITY,
    name                VARCHAR(32)
);

CREATE TABLE order_types
(
    order_type_id INT PRIMARY KEY IDENTITY,
    name          VARCHAR(32)
);

CREATE TABLE customers
(
    customer_id      INT PRIMARY KEY IDENTITY,
    customer_type_id INT,
    name             VARCHAR(32),
    CONSTRAINT customers_customer_types_fk FOREIGN KEY (customer_type_id) REFERENCES customer_types
);

CREATE TABLE products
(
    product_id          INT PRIMARY KEY IDENTITY,
    product_category_id INT,
    part_no             VARCHAR(32)  NOT NULL,
    part_desc           VARCHAR(255) NOT NULL,
    CONSTRAINT products_product_categories_fk FOREIGN KEY (product_category_id) REFERENCES product_categories
);

CREATE TABLE invoices
(
    invoice_id   INT PRIMARY KEY IDENTITY,
    customer_id  INT      NOT NULL,
    invoice_date DATETIME NOT NULL,
    CONSTRAINT invoices_customers_fk FOREIGN KEY (customer_id) REFERENCES customers
);

CREATE TABLE invoice_details
(
    invoice_detail_id INT PRIMARY KEY IDENTITY,
    invoice_id        INT NOT NULL,
    product_id        INT NOT NULL,
    CONSTRAINT invoice_details_invoices_fk FOREIGN KEY (invoice_id) REFERENCES invoices,
    CONSTRAINT invoice_details_products_fk FOREIGN KEY (product_id) REFERENCES products
);

CREATE TABLE deliveries
(
    delivery_id       INT PRIMARY KEY IDENTITY,
    invoice_detail_id INT,
    delivery_date     DATETIME NOT NULL,
    CONSTRAINT deliveries_invoice_details_fk FOREIGN KEY (invoice_detail_id) REFERENCES invoice_details
);

CREATE TABLE orders
(
    order_id      INT PRIMARY KEY IDENTITY,
    customer_id   INT      NOT NULL,
    order_type_id INT,
    order_date    DATETIME NOT NULL,
    CONSTRAINT orders_customers_fk FOREIGN KEY (customer_id) REFERENCES customers,
    CONSTRAINT orders_order_types_fk FOREIGN KEY (order_type_id) REFERENCES order_types
);

CREATE TABLE order_details
(
    order_detail_id INT PRIMARY KEY IDENTITY,
    order_id        INT NOT NULL,
    product_id      INT NOT NULL,
    CONSTRAINT order_details_orders_fk FOREIGN KEY (order_id) REFERENCES orders,
    CONSTRAINT order_details_products_fk FOREIGN KEY (product_id) REFERENCES products
);

CREATE TABLE test_binary
(
    id          INT PRIMARY KEY IDENTITY,
    test_binary BINARY(11),
    test_blob   VARBINARY(MAX)
);
GO
CREATE PROCEDURE GetInvoices @invoice_id INT AS
BEGIN
    SELECT invoice_id, customer_id, invoice_date
    FROM invoices
    WHERE invoice_id = @invoice_id
END
GO
CREATE PROCEDURE GetCustomers @customer_id INT AS
BEGIN
    SELECT customer_id, customer_type_id, name
    FROM customers
    WHERE customer_id = @customer_id
END
GO