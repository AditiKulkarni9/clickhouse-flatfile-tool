// clickhouse-flatfile-tool/handlers/ingestion.go
package handlers

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

type Data struct {
	Price string `gorm:"price"`
}

func IngestData(c *gin.Context) {
	var req struct {
		Source  string   `json:"source"`
		Table   string   `json:"table"`
		Columns []string `json:"columns"`
		Target  string   `json:"target"`
		Output  string   `json:"output"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if clickhouseConn == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Service not initialized"})
		return
	}

	if req.Source == "clickhouse" && req.Target == "flatfile" {
		// Map columns to types
		columnTypes := make(map[string]string)
		for _, col := range req.Columns {
			switch col {
			case "price", "AirlineID", "DepartureDelayGroups", "ArrivalDelayGroups":
				columnTypes[col] = "UInt32"
			case "Year", "PULocationID", "DOLocationID":
				columnTypes[col] = "UInt16"
			case "Quarter", "Month", "DayofMonth", "DayOfWeek", "DistanceGroup", "VendorID", "passenger_count", "RatecodeID", "payment_type":
				columnTypes[col] = "UInt8"
			case "DepDelay", "DepDelayMinutes", "ArrDelay", "ArrDelayMinutes", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge":
				columnTypes[col] = "Float32"
			case "date", "FlightDate", "tpep_pickup_datetime", "tpep_dropoff_datetime":
				columnTypes[col] = "DateTime"
			default:
				columnTypes[col] = "String"
			}
		}

		// Prepare query with type casting
		selectedColumns := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			if columnTypes[col] == "DateTime" {
				selectedColumns[i] = fmt.Sprintf("formatDateTime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') AS %s", col, col)
			} else {
				selectedColumns[i] = col
			}
		}
		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectedColumns, ","), req.Table)
		rows, err := clickhouseConn.Query(c, query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		file, err := os.Create(req.Output)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		if err := writer.Write(req.Columns); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write CSV header: " + err.Error()})
			return
		}

		count := 0
		for rows.Next() {
			valuePtrs := make([]interface{}, len(req.Columns))
			for i, col := range req.Columns {
				switch columnTypes[col] {
				case "UInt32":
					var val uint32
					valuePtrs[i] = &val
				case "UInt16":
					var val uint16
					valuePtrs[i] = &val
				case "UInt8":
					var val uint8
					valuePtrs[i] = &val
				case "Float32":
					var val float32
					valuePtrs[i] = &val
				default:
					var val string
					valuePtrs[i] = &val
				}
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			row := make([]string, len(req.Columns))
			for i, col := range req.Columns {
				switch columnTypes[col] {
				case "UInt32":
					row[i] = fmt.Sprintf("%d", *(valuePtrs[i].(*uint32)))
				case "UInt16":
					row[i] = fmt.Sprintf("%d", *(valuePtrs[i].(*uint16)))
				case "UInt8":
					row[i] = fmt.Sprintf("%d", *(valuePtrs[i].(*uint8)))
				case "Float32":
					row[i] = fmt.Sprintf("%.2f", *(valuePtrs[i].(*float32)))
				default:
					row[i] = *(valuePtrs[i].(*string))
				}
			}
			if err := writer.Write(row); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write CSV row: " + err.Error()})
				return
			}
			count++
		}

		c.JSON(http.StatusOK, gin.H{"message": "Ingestion complete", "recordCount": count})
	} else if req.Source == "flatfile" && req.Target == "clickhouse" {
		file, err := os.Open(req.Table)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open CSV: " + err.Error()})
			return
		}
		defer file.Close()

		reader := csv.NewReader(file)
		headers, err := reader.Read()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read CSV headers: " + err.Error()})
			return
		}

		colIndices := make([]int, len(req.Columns))
		for i, col := range req.Columns {
			found := false
			for j, header := range headers {
				if header == col {
					colIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Column %s not found in CSV", col)})
				return
			}
		}

		// Map CSV columns to ClickHouse types
		columnTypes := make(map[string]string)
		for _, col := range req.Columns {
			switch col {
			case "age", "passenger_count", "VendorID", "RatecodeID", "payment_type":
				columnTypes[col] = "UInt8"
			case "PULocationID", "DOLocationID":
				columnTypes[col] = "UInt16"
			case "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "trip_distance":
				columnTypes[col] = "Float32"
			case "tpep_pickup_datetime", "tpep_dropoff_datetime":
				columnTypes[col] = "DateTime"
			default:
				columnTypes[col] = "String"
			}
		}

		placeholders := strings.Repeat("?, ", len(req.Columns))
		placeholders = placeholders[:len(placeholders)-2]
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", req.Output, strings.Join(req.Columns, ","), placeholders)

		count := 0
		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read CSV row: " + err.Error()})
				return
			}

			values := make([]interface{}, len(req.Columns))
			for i, idx := range colIndices {
				switch columnTypes[req.Columns[i]] {
				case "UInt8":
					var val uint8
					fmt.Sscanf(record[idx], "%d", &val)
					values[i] = val
				case "UInt16":
					var val uint16
					fmt.Sscanf(record[idx], "%d", &val)
					values[i] = val
				case "Float32":
					var val float32
					fmt.Sscanf(record[idx], "%f", &val)
					values[i] = val
				case "DateTime":
					values[i] = record[idx] // Assumes format like '2019-01-01 00:46:40'
				default:
					values[i] = record[idx]
				}
			}

			if err := clickhouseConn.Exec(c, query, values...); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert row: " + err.Error()})
				return
			}
			count++
		}

		c.JSON(http.StatusOK, gin.H{"message": "Ingestion complete", "recordCount": count})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source/target combination"})
	}
}
