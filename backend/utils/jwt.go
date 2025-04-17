package utils

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// JWTSecret is the secret key used to sign JWT tokens
// In a production environment, this should be stored securely
var JWTSecret = []byte("clickhouse-flatfile-tool-secret-key")

// GenerateToken creates a new JWT token for the given username
func GenerateToken(username string, expirationHours int) (string, error) {
	// Create the claims
	claims := jwt.MapClaims{
		"user": username,
		"exp":  time.Now().Add(time.Hour * time.Duration(expirationHours)).Unix(),
		"iat":  time.Now().Unix(),
	}

	// Create the token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Sign the token with the secret key
	tokenString, err := token.SignedString(JWTSecret)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %v", err)
	}

	return tokenString, nil
}

// ValidateToken validates a JWT token and returns the claims
func ValidateToken(tokenString string) (jwt.MapClaims, error) {
	// Parse the token
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Validate the signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return JWTSecret, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %v", err)
	}

	// Check if the token is valid
	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	// Extract the claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims format")
	}

	return claims, nil
}
