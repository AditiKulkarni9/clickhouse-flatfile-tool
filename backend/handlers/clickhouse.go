// clickhouse-flatfile-tool/handlers/clickhouse.go
package handlers

import (
    "fmt"
    "net/http"
    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
    "github.com/gin-gonic/gin"
)

var clickhouseConn driver.Conn

func ConnectClickHouse(c *gin.Context) {
    var config struct {
        Host     string `json:"host"`
        Port     string `json:"port"`
        Database string `json:"database"`
        User     string `json:"user"`
        JWToken  string `json:"jwtToken"`
    }
    if err := c.ShouldBindJSON(&config); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{fmt.Sprintf("%s:%s", config.Host, config.Port)},
        Auth: clickhouse.Auth{
            Database: config.Database,
            Username: config.User,
            Password: config.JWToken,
        },
    })
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Connection failed: " + err.Error()})
        return
    }
    if err := conn.Ping(c); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Ping failed: " + err.Error()})
        return
    }

    clickhouseConn = conn
    c.JSON(http.StatusOK, gin.H{"message": "Connected successfully"})
}

func GetClickHouseTables(c *gin.Context) {
    if clickhouseConn == nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Service not initialized"})
        return
    }
    rows, err := clickhouseConn.Query(c, "SHOW TABLES")
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    defer rows.Close()
    tables := []string{}
    for rows.Next() {
        var table string
        if err := rows.Scan(&table); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        tables = append(tables, table)
    }
    c.JSON(http.StatusOK, tables)
}

func GetClickHouseColumns(c *gin.Context) {
    if clickhouseConn == nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Service not initialized"})
        return
    }
    table := c.Param("table")
    rows, err := clickhouseConn.Query(c, "DESCRIBE TABLE "+table)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch columns: " + err.Error()})
        return
    }
    defer rows.Close()
    columns := []map[string]string{}
    for rows.Next() {
        var name, typ, defaultKind, defaultExpr, comment, codec, ttl string
        if err := rows.Scan(&name, &typ, &defaultKind, &defaultExpr, &comment, &codec, &ttl); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to scan column: " + err.Error()})
            return
        }
        columns = append(columns, map[string]string{"name": name, "type": typ})
    }
    c.JSON(http.StatusOK, columns)
}