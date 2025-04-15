// models/connection.go
package models

type ClickHouseConfig struct {
    Host      string `json:"host"`
    Port      string `json:"port"`
    Database  string `json:"database"`
    User      string `json:"user"`
    JWTToken  string `json:"jwtToken"`
}

type FlatFileConfig struct {
    FileName  string `json:"fileName"`
    Delimiter string `json:"delimiter"`
}
