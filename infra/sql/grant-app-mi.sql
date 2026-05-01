-- Grant the App Service system-assigned Managed Identity access to Azure SQL.
-- Run this against the target database while signed in as the SQL AAD admin
-- (e.g. via SSMS, Azure Data Studio, or `sqlcmd -G`).
--
-- Replace <APP_NAME> with the App Service name (also the MI display name),
-- e.g. app-kraken-eyDev. Adjust the role memberships to match least privilege
-- for what the app actually needs.

CREATE USER [<APP_NAME>] FROM EXTERNAL PROVIDER;

ALTER ROLE db_datareader ADD MEMBER [<APP_NAME>];
ALTER ROLE db_datawriter ADD MEMBER [<APP_NAME>];
-- ALTER ROLE db_ddladmin   ADD MEMBER [<APP_NAME>];   -- only if the app issues DDL

-- Sanity check
SELECT name, type_desc, authentication_type_desc
FROM sys.database_principals
WHERE name = '<APP_NAME>';
