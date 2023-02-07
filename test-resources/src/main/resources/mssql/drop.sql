IF DB_ID('dbpop') IS NOT NULL
    BEGIN
        USE dbpop;
        DROP PROCEDURE IF EXISTS GetCustomers
        DROP PROCEDURE IF EXISTS GetInvoices
        DROP TABLE IF EXISTS test_binary
        DROP TABLE IF EXISTS order_details
        DROP TABLE IF EXISTS orders
        DROP TABLE IF EXISTS deliveries
        DROP TABLE IF EXISTS invoice_details
        DROP TABLE IF EXISTS invoices
        DROP TABLE IF EXISTS products
        DROP TABLE IF EXISTS customers
        DROP TABLE IF EXISTS order_types
        DROP TABLE IF EXISTS product_categories
        DROP TABLE IF EXISTS customer_types
        DROP TABLE IF EXISTS test_new_table
    END

