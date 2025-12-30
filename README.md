# Minimal On-Prem Lakehouse (Iceberg + Polaris + MinIO + Trino)

A minimal, reproducible on-prem lakehouse setup centered around **Apache Iceberg** tables.

- **Polaris** manages Iceberg metadata and RBAC through a REST catalog  
- **MinIO** stores data and metadata using S3-compatible object storage  
- **Trino** exposes Iceberg tables via SQL  

This repository is the companion implementation for the Medium article below.

👉 **Medium post**  
https://medium.com/@mustafaoncu815/from-architecture-to-execution-running-an-on-prem-lakehouse-with-iceberg-polaris-minio-and-trino-6e39317072d0

---

## Architecture

- **Table format**: Apache Iceberg  
- **Catalog**: Apache Polaris (Iceberg REST Catalog)  
- **Storage**: MinIO (S3-compatible)  
- **Query engine**: Trino  

Iceberg is the core of the system.  
Everything else exists to store, manage, or query Iceberg tables.

---

## Requirements

- Docker  
- Docker Compose  

---

## Quick Start

### 1. Start the stack
```bash
docker compose up -d
