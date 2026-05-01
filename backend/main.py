"""
Minimal FastAPI app — proof of life for the Kraken Accelerator backend.

Endpoints:
    GET /             - basic banner
    GET /health       - liveness probe
    GET /sql/ping     - confirms Azure SQL is reachable
    GET /blob/ping    - confirms Blob Storage is reachable + lists containers

Run locally:
    uvicorn main:app --reload --port 8000

Deployed in App Service the same code uses the system-assigned managed identity
instead of your local az-login session.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config import settings


app = FastAPI(title="Kraken Accelerator API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


_credential = DefaultAzureCredential()


def _sql_connection() -> pyodbc.Connection:
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{settings.azure_sql_server},1433;"
        f"Database={settings.azure_sql_database};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        "Authentication=ActiveDirectoryDefault;"
    )
    return pyodbc.connect(conn_str)


@app.get("/api")
def api_root() -> dict:
    return {
        "service": "kraken-accelerator-api",
        "version": "0.1.0",
        "now": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/healthz")
def healthz() -> dict:
    """Kubernetes/App Service-style health probe used by the platform."""
    return {"status": "ok"}


@app.get("/sql/ping")
def sql_ping() -> dict:
    try:
        with _sql_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT SUSER_SNAME(), SYSDATETIME();")
            user, now = cur.fetchone()
            return {
                "ok": True,
                "server": settings.azure_sql_server,
                "database": settings.azure_sql_database,
                "connected_as": user,
                "server_time": str(now),
            }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"{type(exc).__name__}: {exc}")


@app.get("/blob/ping")
def blob_ping() -> dict:
    try:
        client = BlobServiceClient(
            account_url=f"https://{settings.azure_storage_account}.blob.core.windows.net",
            credential=_credential,
        )
        containers = [c.name for c in client.list_containers()]
        return {
            "ok": True,
            "account": settings.azure_storage_account,
            "containers": containers,
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"{type(exc).__name__}: {exc}")


_static_dir = Path(os.environ.get("STATIC_DIR", Path(__file__).parent / "static"))
if _static_dir.is_dir() and any(_static_dir.iterdir()):
    app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
else:
    @app.get("/")
    def root() -> dict:
        return {
            "service": "kraken-accelerator-api",
            "version": "0.1.0",
            "now": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "note": "no static frontend bundled; mount one at backend/static/",
        }
