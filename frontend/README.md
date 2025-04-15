# ClickHouse & Flat File Data Ingestion Tool

A web-based application for bidirectional data ingestion between ClickHouse and Flat Files.

## Setup

1. **Prerequisites**:
   - Go 1.18+
   - Node.js 18+
   - Docker

2. **Run ClickHouse**:
   ```bash
   docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server

3. **Backend**
    ```bash
    cd backend
    mkdir uploads
    go mod tidy
    go run main.go

4. **Frontend**
    ```bash
    cd frontend
    npm install
    npm run dev

5. **Access** : http://localhost:5173

Features: 
Bidirectional ingestion (ClickHouse ↔ Flat File).
Column selection.
Record count reporting.
Basic error handling.
Limitations
JWT authentication uses token as password (ClickHouse config adjustment needed for proper JWT).
No multi-table JOIN or data preview implemented.