# ClickHouse ↔ Flat File Ingestion Tool

A robust bidirectional data ingestion tool for seamless data transfer between ClickHouse and CSV files. Built with a Go backend and a React frontend styled with Tailwind CSS, this tool provides an intuitive interface for exporting ClickHouse tables to CSV and importing CSV files into ClickHouse.

## Features

- **Export ClickHouse to CSV**: Export tables (e.g., `uk_price_paid`) to CSV files with customizable column selection.
- **Import CSV to ClickHouse**: Ingest CSV files into ClickHouse tables (e.g., `uk_price_paid_import`) with type-aware data mapping.
- **Interactive UI**: Features "Load Columns" and "Preview" functionality for easy data inspection and validation.
- **Type-Aware Ingestion**: Automatically maps CSV data to ClickHouse data types for accurate imports.
- **Robust Error Handling**: Validates CSV headers, handles connection issues, and provides clear error messages.

## Prerequisites

- **Docker**: For running the ClickHouse server.
- **Go**: Version 1.21 or higher.
- **Node.js**: Version 18 or higher.
- **ClickHouse Server**: A running ClickHouse instance.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/AditiKulkarni9/clickhouse-flatfile-tool
   cd clickhouse-flatfile-tool

2. **Start Clickhouse server**
    ```bash
    docker run -d --name clickhouse-server -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server

3. **Run Backend:**
    ```bash
    cd backend
    go mod tidy
    go run main.go

4. **Run Frontend:**
    ```bash
    cd frontend
    npm install
    npm run dev


## Usage
- Access the Application: Open your browser and navigate to http://localhost:5173.
- ClickHouse to CSV Export:
- Select ClickHouse as the source and CSV as the target. <br>
**Username: default** <br>
**Password: (leave empty if you haven't set a password, or enter your password if you have)**<br>
- Choose a table (e.g., uk_price_paid).
- Click Load Columns to view available columns.
- Select desired columns (e.g., price, postcode1).
- Click Preview to inspect the data.
- Click Export to download the CSV file.
- Select "Flat File" as the source
- Select "ClickHouse" as the target
- Click "Connect" to establish the ClickHouse - connection
- Upload the test CSV file (backend/uploads/test_uk_price_paid.csv)
- Click "Load Columns" to see the available columns
- Select all columns (price, street, date)
- Click "Preview" to verify the data
- Click "Start Ingestion" to begin the ingestion process

![My Image](frontend/src/assets/Screenshot%202025-04-17%20at%2012.46.51.png)

![My Image](frontend/src/assets/Screenshot%202025-04-17%20at%2017.11.09.png)

![My Image](frontend/src/assets/Screenshot%202025-04-17%20at%2017.10.13.png)

![My Image](frontend/src/assets/Screenshot%202025-04-17%20at%2017.53.34.png)
