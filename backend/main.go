package main

import (
	"log"

	"github.com/AditiKulkarni9/clickhouse-flatfile-tool/handlers"
	"github.com/gin-gonic/gin"
)

func main() {
	router := setupRouter()
	log.Println("Starting server...")
	router.Run(":8080")
}

func setupRouter() *gin.Engine {
	router := gin.Default()

	// Enable CORS for frontend
	router.Use(func(c *gin.Context) {
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
	router.POST("/connect/clickhouse", handlers.ConnectClickHouse)
	router.GET("/tables/clickhouse", handlers.GetClickHouseTables)
	router.GET("/columns/clickhouse/:table", handlers.GetClickHouseColumns)
	router.POST("/upload/flatfile", handlers.UploadFlatFile)
	router.GET("/columns/flatfile", handlers.GetFlatFileColumns)
	router.POST("/ingest", handlers.IngestData)
	router.POST("/preview", handlers.PreviewData)
	router.POST("/auth/token", handlers.GenerateJWTToken)

	return router
}
