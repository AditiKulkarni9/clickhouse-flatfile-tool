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
        // Cast DateTime columns to String
        selectedColumns := make([]string, len(req.Columns))
        for i, col := range req.Columns {
            if col == "date" {
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
            values := make([]interface{}, len(req.Columns))
            valuePtrs := make([]interface{}, len(req.Columns))
            for i := range values {
                var val string
                valuePtrs[i] = &val
            }
            if err := rows.Scan(valuePtrs...); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            row := make([]string, len(req.Columns))
            for i := range req.Columns {
                row[i] = *(valuePtrs[i].(*string))
            }
            if err := writer.Write(row); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write CSV row: " + err.Error()})
                return
            }
            count++
        }

        c.JSON(http.StatusOK, gin.H{"message": "Ingestion complete", "recordCount": count})
    } else if req.Source == "flatfile" && req.Target == "clickhouse" {
        // Read CSV file
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

        // Map requested columns to indices
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

        // Prepare ClickHouse query
        placeholders := strings.Repeat("?, ", len(req.Columns))
        placeholders = placeholders[:len(placeholders)-2] // Remove trailing ", "
        query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", req.Output, strings.Join(req.Columns, ","), placeholders)

        // Read and insert rows
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
                if req.Columns[i] == "age" {
                    var age uint32
                    fmt.Sscanf(record[idx], "%d", &age)
                    values[i] = age
                } else {
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