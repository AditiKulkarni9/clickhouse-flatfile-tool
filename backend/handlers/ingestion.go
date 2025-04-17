package handlers

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// getColumnTypes fetches column types from system.columns
func getColumnTypes(c *gin.Context, database, table string) (map[string]string, error) {
	query := `
		SELECT name, type
		FROM system.columns
		WHERE database = ? AND table = ?
	`
	rows, err := clickhouseConn.Query(c, query, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query column types: %v", err)
	}
	defer rows.Close()

	columnTypes := make(map[string]string)
	for rows.Next() {
		var name, typeStr string
		if err := rows.Scan(&name, &typeStr); err != nil {
			return nil, fmt.Errorf("failed to scan column types: %v", err)
		}
		switch {
		case strings.HasPrefix(typeStr, "UInt32"):
			columnTypes[name] = "UInt32"
		case strings.HasPrefix(typeStr, "UInt16"):
			columnTypes[name] = "UInt16"
		case strings.HasPrefix(typeStr, "UInt8"):
			columnTypes[name] = "UInt8"
		case strings.HasPrefix(typeStr, "Float32"):
			columnTypes[name] = "Float32"
		case strings.HasPrefix(typeStr, "DateTime"):
			columnTypes[name] = "DateTime"
		case strings.HasPrefix(typeStr, "Enum"):
			columnTypes[name] = "String"
		default:
			columnTypes[name] = "String"
		}
	}
	return columnTypes, nil
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

	database := "uk"
	tableName := req.Table
	outputTable := req.Output
	if req.Table == "uk_price_paid" {
		tableName = "uk.uk_price_paid"
	}
	if req.Output == "uk_price_paid_import" {
		outputTable = "uk.uk_price_paid_import"
	}

	if req.Source == "clickhouse" && req.Target == "flatfile" {
		simpleTable := strings.TrimPrefix(tableName, "uk.")
		columnTypes, err := getColumnTypes(c, database, simpleTable)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		for _, col := range req.Columns {
			if _, exists := columnTypes[col]; !exists {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Column %s not found in table %s", col, tableName)})
				return
			}
		}

		selectedColumns := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			if columnTypes[col] == "DateTime" {
				selectedColumns[i] = fmt.Sprintf("formatDateTime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') AS %s", col, col)
			} else {
				selectedColumns[i] = col
			}
		}
		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectedColumns, ","), tableName)
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

		// Create a map of CSV headers to their indices
		headerMap := make(map[string]int)
		for i, header := range headers {
			headerMap[header] = i
		}

		// Get target table column types
		simpleOutputTable := strings.TrimPrefix(outputTable, "uk.")
		columnTypes, err := getColumnTypes(c, database, simpleOutputTable)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Validate and map columns
		colIndices := make([]int, len(req.Columns))
		for i, col := range req.Columns {
			if idx, exists := headerMap[col]; exists {
				colIndices[i] = idx
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Column %s not found in CSV. Available headers: %v", col, headers)})
				return
			}
		}

		// Prepare the INSERT query
		placeholders := strings.Repeat("?, ", len(req.Columns))
		placeholders = placeholders[:len(placeholders)-2]
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", outputTable, strings.Join(req.Columns, ","), placeholders)

		count := 0
		batchSize := 1000
		batch := make([][]interface{}, 0, batchSize)

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
				col := req.Columns[i]
				value := record[idx]

				switch columnTypes[col] {
				case "UInt32":
					var val uint32
					if _, err := fmt.Sscanf(value, "%d", &val); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid UInt32 value for column %s: %s", col, value)})
						return
					}
					values[i] = val
				case "UInt16":
					var val uint16
					if _, err := fmt.Sscanf(value, "%d", &val); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid UInt16 value for column %s: %s", col, value)})
						return
					}
					values[i] = val
				case "UInt8":
					var val uint8
					if _, err := fmt.Sscanf(value, "%d", &val); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid UInt8 value for column %s: %s", col, value)})
						return
					}
					values[i] = val
				case "Float32":
					var val float32
					if _, err := fmt.Sscanf(value, "%f", &val); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid Float32 value for column %s: %s", col, value)})
						return
					}
					values[i] = val
				case "DateTime":
					// For String type, just pass the value directly
					values[i] = value
				default:
					values[i] = value
				}
			}

			batch = append(batch, values)
			count++

			// Execute batch insert when batch size is reached
			if len(batch) >= batchSize {
				if err := executeBatch(c, query, batch); err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert batch: " + err.Error()})
					return
				}
				batch = batch[:0]
			}
		}

		// Execute remaining records
		if len(batch) > 0 {
			if err := executeBatch(c, query, batch); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert final batch: " + err.Error()})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{"message": "Ingestion complete", "recordCount": count})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source/target combination"})
	}
}

func executeBatch(c *gin.Context, query string, batch [][]interface{}) error {
	for _, values := range batch {
		if err := clickhouseConn.Exec(c, query, values...); err != nil {
			return err
		}
	}
	return nil
}
