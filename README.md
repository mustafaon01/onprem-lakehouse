# Minimal On-Prem Lakehouse Core

A compact on-prem stack centered around Iceberg tables: Polaris (Iceberg REST Catalog) manages metadata, MinIO stores the data, and Trino exposes the tables. This README and `./scripts/bootstrap.py` are short, shareable references for your Medium post.

## Architecture
- **Storage**: MinIO (S3-compatible)
- **Catalog**: Apache Polaris (Iceberg REST Catalog)
- **Processing**: Polaris REST Catalog (Iceberg metadata + RBAC)
- **Query**: Trino (Iceberg REST connector)

## Requirements
- Docker & Docker Compose

## Quick Start

### 1. Start the stack
```bash
docker compose up -d
```
*Give MinIO and Polaris ~30 seconds to boot.*

### 2. Bootstrap Polaris
`./scripts/bootstrap.py` creates the catalog, principals, and RBAC bindings automatically.
```bash
python scripts/bootstrap.py
```
This script is the content core for your Medium article because it automates Polaris API calls and RBAC wiring.

### 3. Validate through Trino
Query the data Polaris already exposes.
```bash
docker exec -it trino trino
```

Run the following SQL commands:

```sql
-- Check schemas
SHOW SCHEMAS FROM iceberg;

-- Check tables
SHOW TABLES FROM iceberg;

-- Select data
-- Replace <table_name> with any available table
SELECT * FROM iceberg.<table_name> LIMIT 10;
```

## Polaris Management API (example)
Get a token with `default-realm, root/secret`, then create and inspect catalogs:
```bash
ACCESS_TOKEN=$(curl -X POST \
  http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')

# Create catalog
curl -i -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8181/api/management/v1/catalogs \
  --json '{
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse",
      "s3.endpoint": "http://minio:9000",
      "s3.path-style-access": "true",
      "s3.access-key-id": "admin",
      "s3.secret-access-key": "password",
      "s3.region": "dummy-region"
    },
    "storageConfigInfo": {
      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/*"]
    }
  }'

# List catalogs
curl -X GET http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq

# Inspect root principal roles
curl -X GET http://localhost:8181/api/management/v1/principals/root/principal-roles \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

## Config Reference
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Trino UI**: http://localhost:8080 (user: 'admin')
- **Configs**:
  - `trino/catalog/iceberg.properties`: Trino settings for Polaris REST + MinIO.
