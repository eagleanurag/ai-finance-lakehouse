from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import requests

from config import settings
from src.sources.file_ingest import SUPPORTED_EXTENSIONS, normalize_columns, read_tabular


def sharepoint_mode() -> str:
    if all(
        [
            settings.SHAREPOINT_TENANT_ID,
            settings.SHAREPOINT_CLIENT_ID,
            settings.SHAREPOINT_CLIENT_SECRET,
            settings.SHAREPOINT_SITE_HOST,
        ]
    ):
        return "graph_api"
    return "local_export"


def fetch_sharepoint_files() -> list[dict[str, Any]]:
    if sharepoint_mode() == "graph_api":
        return _fetch_via_graph()
    return _read_local_exports()


def _fetch_via_graph() -> list[dict[str, Any]]:
    token = _get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    drive = settings.SHAREPOINT_DRIVE_ID
    folder_path = settings.SHAREPOINT_FOLDER_PATH
    base = f"https://graph.microsoft.com/v1.0/drives/{drive}/root:/{folder_path}:/children"
    results: list[dict[str, Any]] = []
    while base:
        resp = requests.get(base, headers=headers, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        for item in payload.get("value", []):
            if item.get("folder") or not any(
                item["name"].lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS
            ):
                continue
            download = item["@microsoft.graph.downloadUrl"]
            file_resp = requests.get(download, timeout=120)
            file_resp.raise_for_status()
            tmp = Path(tempfile.gettempdir()) / item["name"]
            tmp.write_bytes(file_resp.content)
            df = normalize_columns(read_tabular(tmp))
            results.append({"source_file": item["name"], "records": df.to_dict(orient="records"), "rows": len(df)})
            tmp.unlink(missing_ok=True)
        base = payload.get("@odata.nextLink")
    return results


def _get_graph_token() -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{settings.SHAREPOINT_TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": settings.SHAREPOINT_CLIENT_ID,
            "client_secret": settings.SHAREPOINT_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _read_local_exports() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in sorted(settings.SHAREPOINT_EXPORT_DIR.glob("*")):
        if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue
        df = normalize_columns(read_tabular(path))
        results.append({"source_file": path.name, "records": df.to_dict(orient="records"), "rows": len(df)})
    return results
