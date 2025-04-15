package services

import (
	"fmt"
    "encoding/csv"
    "os"
    "github.com/AditiKulkarni9/clickhouse-flatfile-tool/models"
)

type FlatFileService struct {
    filePath  string
    delimiter string
}

func NewFlatFileService(filePath, delimiter string) *FlatFileService {
    return &FlatFileService{filePath: filePath, delimiter: delimiter}
}

func (s *FlatFileService) GetColumns() ([]models.Column, error) {
    file, err := os.Open(s.filePath)
    if err != nil {
        return nil, fmt.Errorf("failed to open file: %v", err)
    }
    defer file.Close()

    reader := csv.NewReader(file)
    reader.Comma = rune(s.delimiter[0])
    headers, err := reader.Read()
    if err != nil {
        return nil, fmt.Errorf("failed to read headers: %v", err)
    }

    var columns []models.Column
    for _, header := range headers {
        columns = append(columns, models.Column{Name: header, Type: "String"}) // Assume String for simplicity
    }
    return columns, nil
}