import os
import sys
import json
import time
from typing import Any, Dict, Optional

import requests

POLARIS = os.getenv("POLARIS_URL", "http://localhost:8181")

# OAuth client credentials
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "secret")
SCOPE = os.getenv("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")

# Catalog config
CATALOG_NAME = os.getenv("POLARIS_CATALOG", "polariscatalog")

S3_ENDPOINT = os.getenv("POLARIS_S3_ENDPOINT", "http://minio:9000")
S3_REGION = os.getenv("POLARIS_S3_REGION", "dummy-region")
S3_ACCESS_KEY = os.getenv("POLARIS_S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("POLARIS_S3_SECRET_KEY", "password")
DEFAULT_BASE_LOCATION = os.getenv("POLARIS_DEFAULT_BASE_LOCATION", "s3://warehouse")

ROLE_ARN = os.getenv(
    "POLARIS_ROLE_ARN",
    "arn:aws:iam::000000000000:role/minio-polaris-role",
)
ALLOWED_LOCATIONS = os.getenv("POLARIS_ALLOWED_LOCATIONS", "s3://warehouse/*").split(",")

# RBAC names
CATALOG_ROLE = os.getenv("POLARIS_CATALOG_ROLE", "catalog_admin")
PRINCIPAL_ROLE = os.getenv("POLARIS_PRINCIPAL_ROLE", "data_engineer")
PRINCIPAL = os.getenv("POLARIS_PRINCIPAL", "root")

TIMEOUT = 15


def die(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)


def pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


def request_json(
        method: str,
        url: str,
        token: Optional[str] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data_body: Optional[str] = None,
        ok_statuses=(200, 201, 204),
        tolerate_statuses=(409,),  # already exists
) -> Optional[Dict[str, Any]]:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if json_body is not None:
        headers["Content-Type"] = "application/json"
        resp = requests.request(method, url, headers=headers, json=json_body, timeout=TIMEOUT)
    elif data_body is not None:
        # OAuth endpoint expects x-www-form-urlencoded
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        resp = requests.request(method, url, headers=headers, data=data_body, timeout=TIMEOUT)
    else:
        resp = requests.request(method, url, headers=headers, timeout=TIMEOUT)

    if resp.status_code in ok_statuses or resp.status_code in tolerate_statuses:
        if resp.text.strip():
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text}
        return None

    # Helpful error
    body = resp.text.strip()
    die(f"{method} {url} -> {resp.status_code}\n{body}")


def wait_until_up() -> None:
    # Polaris bazen boot ederken 503/5xx dönebilir. Basit wait.
    for _ in range(60):
        try:
            r = requests.get(POLARIS, timeout=5)
            if r.status_code < 500:
                return
        except Exception:
            pass
        time.sleep(1)
    die("Polaris is not reachable on time.")


def get_access_token() -> str:
    url = f"{POLARIS}/api/catalog/v1/oauth/tokens"
    form = (
        "grant_type=client_credentials"
        f"&client_id={CLIENT_ID}"
        f"&client_secret={CLIENT_SECRET}"
        f"&scope={SCOPE}"
    )
    data = request_json("POST", url, data_body=form, ok_statuses=(200, 201))
    if not data or "access_token" not in data:
        die(f"Token response missing access_token: {pretty(data)}")
    return data["access_token"]


def create_catalog(token: str) -> None:
    url = f"{POLARIS}/api/management/v1/catalogs"
    payload = {
        "name": CATALOG_NAME,
        "type": "INTERNAL",
        "properties": {
            "default-base-location": DEFAULT_BASE_LOCATION,
            "s3.endpoint": S3_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
        },
        "storageConfigInfo": {
            "roleArn": ROLE_ARN,
            "storageType": "S3",
            "allowedLocations": ALLOWED_LOCATIONS,
        },
    }
    request_json("POST", url, token=token, json_body=payload, ok_statuses=(200, 201, 204), tolerate_statuses=(409,))
    print(f"[OK] Catalog ensured: {CATALOG_NAME}")


def list_catalogs(token: str) -> Dict[str, Any]:
    url = f"{POLARIS}/api/management/v1/catalogs"
    data = request_json("GET", url, token=token, ok_statuses=(200,))
    return data or {}


def grant_catalog_admin(token: str) -> None:
    url = f"{POLARIS}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles/{CATALOG_ROLE}/grants"
    payload = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    request_json("PUT", url, token=token, json_body=payload, ok_statuses=(200, 201, 204), tolerate_statuses=(409,))
    print(f"[OK] Grant ensured: {CATALOG_ROLE} -> CATALOG_MANAGE_CONTENT")


def create_principal_role(token: str) -> None:
    url = f"{POLARIS}/api/management/v1/principal-roles"
    payload = {"principalRole": {"name": PRINCIPAL_ROLE}}
    request_json("POST", url, token=token, json_body=payload, ok_statuses=(200, 201, 204), tolerate_statuses=(409,))
    print(f"[OK] Principal role ensured: {PRINCIPAL_ROLE}")


def connect_roles(token: str) -> None:
    url = f"{POLARIS}/api/management/v1/principal-roles/{PRINCIPAL_ROLE}/catalog-roles/{CATALOG_NAME}"
    payload = {"catalogRole": {"name": CATALOG_ROLE}}
    request_json("PUT", url, token=token, json_body=payload, ok_statuses=(200, 201, 204), tolerate_statuses=(409,))
    print(f"[OK] Connected: principal_role {PRINCIPAL_ROLE} -> catalog_role {CATALOG_ROLE} on {CATALOG_NAME}")


def assign_role_to_root(token: str) -> None:
    url = f"{POLARIS}/api/management/v1/principals/{PRINCIPAL}/principal-roles"
    payload = {"principalRole": {"name": PRINCIPAL_ROLE}}
    request_json("PUT", url, token=token, json_body=payload, ok_statuses=(200, 201, 204), tolerate_statuses=(409,))
    print(f"[OK] Assigned: {PRINCIPAL} -> {PRINCIPAL_ROLE}")


def list_root_roles(token: str) -> Dict[str, Any]:
    url = f"{POLARIS}/api/management/v1/principals/{PRINCIPAL}/principal-roles"
    data = request_json("GET", url, token=token, ok_statuses=(200,))
    return data or {}


def main():
    wait_until_up()
    token = get_access_token()
    print("[OK] Access token acquired.")

    create_catalog(token)

    catalogs = list_catalogs(token)
    print("[INFO] Catalogs:")
    print(pretty(catalogs))

    # RBAC
    grant_catalog_admin(token)
    create_principal_role(token)
    connect_roles(token)
    assign_role_to_root(token)

    roles = list_root_roles(token)
    print("[INFO] Root principal roles:")
    print(pretty(roles))


if __name__ == "__main__":
    main()
