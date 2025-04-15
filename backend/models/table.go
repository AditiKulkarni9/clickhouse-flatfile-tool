package models

type Table struct {
    Name    string   `json:"name"`
    Columns []Column `json:"columns"`
}

type Column struct {
    Name string `json:"name"`
    Type string `json:"type"`
}

type IngestionRequest struct {
    SourceType     string           `json:"sourceType"`
    TargetType     string           `json:"targetType"`
    ClickHouse     ClickHouseConfig `json:"clickHouse"`
    FlatFile       FlatFileConfig   `json:"flatFile"`
    SelectedTables []string         `json:"selectedTables"`
    SelectedColumns []string        `json:"selectedColumns"`
}