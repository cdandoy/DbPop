DROP TABLE IF EXISTS dbpop.public.invoice_details;
DROP TABLE IF EXISTS dbpop.public.invoices;
DROP TABLE IF EXISTS dbpop.public.products;
DROP TABLE IF EXISTS dbpop.public.customers;

CREATE TABLE dbpop.public.customers
(
    customer_id SERIAL PRIMARY KEY,
    name        VARCHAR(32)
);

CREATE TABLE dbpop.public.products
(
    product_id SERIAL PRIMARY KEY,
    part_no    VARCHAR(32)  NOT NULL,
    part_desc  VARCHAR(255) NOT NULL
);

CREATE TABLE dbpop.public.invoices
(
    invoice_id   SERIAL PRIMARY KEY,
    customer_id  INT       NOT NULL REFERENCES dbpop.public.customers,
    invoice_date TIMESTAMP NOT NULL
);

CREATE TABLE dbpop.public.invoice_details
(
    invoice_detail_id SERIAL PRIMARY KEY,
    invoice_id        INT NOT NULL REFERENCES dbpop.public.invoices,
    product_id        INT NOT NULL REFERENCES dbpop.public.products
);