ClickHouse ↔ Flat File Ingestion Tool

A bidirectional data ingestion tool for ClickHouse and CSV files, built with Go (backend) and React with Tailwind CSS (frontend).

Features





Export ClickHouse tables (e.g., uk_price_paid) to CSV.



Import CSV files to ClickHouse tables (e.g., uk_price_paid_import).



UI with Load Columns and Preview features.



Type-aware ingestion for ClickHouse data types.



Error handling for CSV headers and connections.

Setup

Prerequisites





Docker



Go 1.21+



Node.js 18+



ClickHouse server

Installation





Clone the repository:

git clone <your-repo>
cd clickhouse-flatfile-tool



Start ClickHouse:

docker run -d --name clickhouse-server -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server



***Run backend***:


    cd backend
    go mod tidy
    go run main.go



***Run frontend***:

    cd frontend
    npm install
    npm run dev

Usage





Open http://localhost:5173.



ClickHouse → Flat File:





Select ClickHouse source, CSV target.



Choose uk_price_paid, click "Load Columns".



Select columns (e.g., price, postcode1), click "Preview".



Start ingestion.



Flat File → ClickHouse:





Upload backend/Uploads/uk_price_paid.csv.



Click "Load Columns", select columns (price, street, date).



Click "Preview", then ingest.

Testing





Test CSV: backend/Uploads/uk_price_paid.csv (1000 rows, headers: price,street,date).



Verify:

SELECT COUNT(*) FROM uk.uk_price_paid; -- 25898706