CREATE TABLE [master].[advanced].[products]
(
    [product_id] INT IDENTITY (1,1) NOT NULL,
    [product_category_id] INT,
    [part_no] VARCHAR(32) NOT NULL,
    [part_desc] VARCHAR(255) NOT NULL
)
