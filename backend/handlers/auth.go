package handlers

import (
	"net/http"

	"github.com/AditiKulkarni9/clickhouse-flatfile-tool/utils"
	"github.com/gin-gonic/gin"
)

// GenerateJWTToken generates a new JWT token for the given username
func GenerateJWTToken(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Generate a token that expires in 24 hours
	token, err := utils.GenerateToken(req.Username, 24)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": token})
}
