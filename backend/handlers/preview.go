package handlers

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"github.com/gin-gonic/gin"
	"io"
)

type PreviewRequest struct {
	Source  string   `json:"source"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

type PreviewResponse struct {
	Headers []string   `json:"headers"`
	Rows    [][]string `json:"rows"`
}

func PreviewData(c *gin.Context) {
	var req PreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var rows [][]string
	var headers []string

	if req.Source == "clickhouse" {
		if clickhouseConn == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Not connected to ClickHouse"})
			return
		}
		database := "uk"
		tableName := req.Table
		if req.Table == "uk_price_paid" {
			tableName = "uk.uk_price_paid"
		}
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
		query := fmt.Sprintf("SELECT %s FROM %s LIMIT 5", strings.Join(selectedColumns, ","), tableName)
		dbRows, err := clickhouseConn.Query(c, query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer dbRows.Close()

		headers = req.Columns
		for dbRows.Next() {
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
			if err := dbRows.Scan(valuePtrs...); err != nil {
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
			rows = append(rows, row)
		}
	} else if req.Source == "flatfile" {
		filePath := filepath.Join("Uploads", filepath.Base(req.Table))
		file, err := os.Open(filePath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open CSV: " + err.Error()})
			return
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.Comma = ','
		csvHeaders, err := reader.Read()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read CSV headers: " + err.Error()})
			return
		}

		headerMap := make(map[string]int)
		for i, h := range csvHeaders {
			headerMap[h] = i
		}
		headers = req.Columns
		for _, col := range req.Columns {
			if _, ok := headerMap[col]; !ok {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Column %s not found in CSV", col)})
				return
			}
		}

		count := 0
		for count < 5 {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read CSV: " + err.Error()})
				return
			}
			row := make([]string, len(req.Columns))
			for i, col := range req.Columns {
				row[i] = record[headerMap[col]]
			}
			rows = append(rows, row)
			count++
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source type"})
		return
	}

	c.JSON(http.StatusOK, PreviewResponse{
		Headers: headers,
		Rows:    rows,
	})
}