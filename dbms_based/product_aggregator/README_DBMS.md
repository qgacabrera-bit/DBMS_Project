# DBMS-based Product Aggregator (SQL Server)

This copy uses Microsoft SQL Server for cache persistence instead of CSV files.

## 1) Install dependencies

```powershell
pip install -r requirements.txt
```

## 2) Configure SQL Server connection

Set values in `.env`:

- `MSSQL_DRIVER`
- `MSSQL_SERVER`
- `MSSQL_DATABASE`
- `MSSQL_TRUSTED_CONNECTION`
- `MSSQL_ENCRYPT`
- `MSSQL_TRUST_SERVER_CERTIFICATE`

You may also use a full connection string with:

- `MSSQL_CONNECTION_STRING`

## 3) Create database/schema (optional)

Run `db_setup.sql` in SQL Server Management Studio.

The DBMS workflow is aligned to these ERD tables:

- `dbo.Platform`
- `dbo.Category`
- `dbo.Review`
- `dbo.Product`
- `dbo.PriceHistory`

Note: the app will also auto-create these tables at runtime if permissions allow.

## 4) Run app

```powershell
python app.py
```

## Behavior changes

- Search/admin cache operations now persist to SQL Server.
- Existing route paths and admin actions remain the same.
- UI text is updated to SQL/cache wording.
