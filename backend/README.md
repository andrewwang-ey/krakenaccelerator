# Kraken Accelerator API

Minimal FastAPI service that proves connectivity to Azure SQL and Azure Blob Storage. Authentication uses `DefaultAzureCredential`, which picks up:

- **locally** — your `az login` session via the Azure CLI
- **in App Service** — the system-assigned managed identity

The same code works in both environments. No connection strings, no secrets in code.

## Local setup (Windows)

1. Install the Microsoft ODBC Driver 18 for SQL Server (one-time):

   ```powershell
   winget install -e --id Microsoft.MsOdbcSql.18
   ```

2. Sign in to Azure:

   ```powershell
   az login
   ```

3. Create + activate a virtualenv and install requirements:

   ```powershell
   cd backend
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

4. Copy the env template and fill in your values:

   ```powershell
   copy .env.example .env
   notepad .env
   ```

   Set `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_STORAGE_ACCOUNT` to the names you used in the Azure portal.

5. Verify connectivity:

   ```powershell
   python test_connections.py
   ```

   You should see green `[OK]` lines for both SQL and Blob.

6. Run the API locally:

   ```powershell
   uvicorn main:app --reload --port 8000
   ```

   Browse to <http://localhost:8000/docs> for the interactive Swagger UI. Try `/sql/ping` and `/blob/ping`.

## Endpoints

| Method | Path         | Purpose                                   |
| ------ | ------------ | ----------------------------------------- |
| GET    | `/`          | Service banner with version + timestamp   |
| GET    | `/health`    | Cheap liveness probe                      |
| GET    | `/sql/ping`  | Connects to Azure SQL, returns who-am-I   |
| GET    | `/blob/ping` | Lists containers in the storage account   |

## Permissions you need

- **Azure SQL**: be the Microsoft Entra admin on the SQL server (set during create-time, can be changed under SQL server > Microsoft Entra ID).
- **Blob Storage**: hold `Storage Blob Data Contributor` on the storage account (Storage account > Access control (IAM) > Add role assignment).

If `test_connections.py` fails it prints which permission is likely missing.
