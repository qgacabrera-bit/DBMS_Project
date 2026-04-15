IF DB_ID(N'ProductMonitoringDB') IS NULL
BEGIN
    CREATE DATABASE ProductMonitoringDB;
END
GO

USE ProductMonitoringDB;
GO

IF OBJECT_ID(N'dbo.Platform', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Platform (
        platform_id INT IDENTITY(1,1) PRIMARY KEY,
        platform_name NVARCHAR(100) NOT NULL,
        base_url NVARCHAR(500) NULL
    );
END
GO

IF OBJECT_ID(N'dbo.Category', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Category (
        category_id INT IDENTITY(1,1) PRIMARY KEY,
        search_query_name NVARCHAR(255) NOT NULL,
        category_name NVARCHAR(100) NOT NULL
    );
END
GO

IF OBJECT_ID(N'dbo.Review', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Review (
        review_id INT IDENTITY(1,1) PRIMARY KEY,
        rating DECIMAL(10,8) NOT NULL DEFAULT (0),
        review_count INT NOT NULL DEFAULT (0)
    );
END
GO

IF OBJECT_ID(N'dbo.Product', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Product (
        product_id INT IDENTITY(1,1) PRIMARY KEY,
        product_name NVARCHAR(500) NOT NULL,
        current_price DECIMAL(10,2) NOT NULL DEFAULT (0),
        review_id INT NULL,
        category_id INT NOT NULL,
        platform_id INT NOT NULL,
        product_url NVARCHAR(500) NULL,
        date_first_scraped DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_Product_Review FOREIGN KEY (review_id) REFERENCES dbo.Review(review_id),
        CONSTRAINT FK_Product_Category FOREIGN KEY (category_id) REFERENCES dbo.Category(category_id),
        CONSTRAINT FK_Product_Platform FOREIGN KEY (platform_id) REFERENCES dbo.Platform(platform_id)
    );
END
GO

IF OBJECT_ID(N'dbo.PriceHistory', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.PriceHistory (
        price_id INT IDENTITY(1,1) PRIMARY KEY,
        product_id INT NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        date_recorded DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_PriceHistory_Product FOREIGN KEY (product_id) REFERENCES dbo.Product(product_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Category_search_query_name'
      AND object_id = OBJECT_ID(N'dbo.Category')
)
BEGIN
    CREATE INDEX IX_Category_search_query_name
    ON dbo.Category(search_query_name);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_PriceHistory_product_id_date'
      AND object_id = OBJECT_ID(N'dbo.PriceHistory')
)
BEGIN
    CREATE INDEX IX_PriceHistory_product_id_date
    ON dbo.PriceHistory(product_id, date_recorded DESC);
END
GO
