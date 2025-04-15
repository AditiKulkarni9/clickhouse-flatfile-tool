package main

import (
    "github.com/gin-gonic/gin"
    "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
    "github.com/AditiKulkarni9/clickhouse-flatfile-tool/handlers"
)

var clickhouseConn driver.Conn

func main() {
    r := gin.Default()

    // Enable CORS for frontend
    r.Use(func(c *gin.Context) {
        c.Header("Access-Control-Allow-Origin", "http://localhost:5173")
        c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        c.Header("Access-Control-Allow-Headers", "Content-Type")
        if c.Request.Method == "OPTIONS" {
            c.AbortWithStatus(204)
            return
        }
        c.Next()
    })

    // Routes
    r.POST("/connect/clickhouse", handlers.ConnectClickHouse)
    r.GET("/tables/clickhouse", handlers.GetClickHouseTables)
    r.GET("/columns/clickhouse/:table", handlers.GetClickHouseColumns)
    r.POST("/upload/flatfile", handlers.UploadFlatFile)
    r.GET("/columns/flatfile", handlers.GetFlatFileColumns)
    r.POST("/ingest", handlers.IngestData)

    r.Run(":8080")
}