"""
Sanity checks for Azure SQL Database and Azure Blob Storage.

Run locally with:
    python test_connections.py

Locally it picks up your `az login` session via DefaultAzureCredential.
In Azure App Service it picks up the system-assigned managed identity.
The same code works in both places.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from config import settings


GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


def ok(msg: str) -> None:
    print(f"{GREEN}[OK]{RESET}    {msg}")


def fail(msg: str) -> None:
    print(f"{RED}[FAIL]{RESET}  {msg}")


def test_sql() -> bool:
    """Connect to Azure SQL with Entra auth and run a trivial query."""
    print("\n--- Azure SQL ---")
    print(f"Server:   {settings.azure_sql_server}")
    print(f"Database: {settings.azure_sql_database}")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{settings.azure_sql_server},1433;"
        f"Database={settings.azure_sql_database};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        "Authentication=ActiveDirectoryDefault;"
    )
    try:
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()
            cur.execute("SELECT @@VERSION, SUSER_SNAME(), SYSDATETIME();")
            version, user, now = cur.fetchone()
            ok(f"Connected as: {user}")
            ok(f"Server time:  {now}")
            ok(f"Version:      {version.splitlines()[0]}")
            return True
    except pyodbc.InterfaceError as exc:
        fail(f"ODBC driver missing or wrong version: {exc}")
        print("    Install: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server")
        return False
    except pyodbc.OperationalError as exc:
        fail(f"Network/firewall issue: {exc}")
        print("    Check the SQL Server's Networking blade — is your client IP allowed?")
        return False
    except pyodbc.ProgrammingError as exc:
        fail(f"Auth or DB issue: {exc}")
        print("    Have you set yourself as the Entra admin on the SQL Server?")
        return False
    except Exception as exc:
        fail(f"Unexpected: {type(exc).__name__}: {exc}")
        return False


def test_blob() -> bool:
    """Connect to Azure Blob Storage with Entra auth, list containers, write+read+delete a probe blob."""
    print("\n--- Azure Blob Storage ---")
    account_url = f"https://{settings.azure_storage_account}.blob.core.windows.net"
    print(f"Account URL: {account_url}")

    try:
        credential = DefaultAzureCredential()
        client = BlobServiceClient(account_url=account_url, credential=credential)

        containers = [c.name for c in client.list_containers()]
        ok(f"Containers visible: {containers}")

        expected = {"raw", "staged", "rejections", "mappings", "schema"}
        missing = expected - set(containers)
        if missing:
            fail(f"Missing containers: {sorted(missing)}")
            print("    Create them in the portal under Data storage > Containers.")
            return False

        probe_name = f"_probe_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.txt"
        container = client.get_container_client("schema")
        blob = container.get_blob_client(probe_name)
        blob.upload_blob(b"connectivity probe", overwrite=True)
        ok(f"Wrote probe blob: schema/{probe_name}")

        body = blob.download_blob().readall().decode("utf-8")
        ok(f"Read it back:     '{body}'")

        blob.delete_blob()
        ok("Deleted probe blob.")
        return True
    except Exception as exc:
        fail(f"{type(exc).__name__}: {exc}")
        msg = str(exc)
        if "AuthorizationPermissionMismatch" in msg or "403" in msg:
            print("    You likely don't have 'Storage Blob Data Contributor' on the account.")
            print("    Storage account > Access control (IAM) > Add role assignment.")
        elif "AADSTS" in msg or "DefaultAzureCredential failed" in msg:
            print("    Run `az login` first (DefaultAzureCredential picks up the CLI session).")
        return False


def main() -> int:
    print(f"Kraken backend connectivity test  -  {datetime.now().isoformat(timespec='seconds')}")
    sql_ok = test_sql()
    blob_ok = test_blob()
    print()
    if sql_ok and blob_ok:
        ok("All checks passed.")
        return 0
    fail("One or more checks failed (see above).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
