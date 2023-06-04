CREATE TABLE [master].[dbo].[products]
(
    [product_id] INT IDENTITY (1,1) NOT NULL,
    [part_no] VARCHAR(32) NOT NULL,
    [part_desc] VARCHAR(255) NOT NULL
)
