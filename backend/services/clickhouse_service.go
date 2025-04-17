package services

import (
	"context"
	"fmt"

	"github.com/AditiKulkarni9/clickhouse-flatfile-tool/models"
	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseService struct {
	Conn clickhouse.Conn
}

func NewClickHouseService(config models.ClickHouseConfig) (*ClickHouseService, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.User,
			Password: config.Password,
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}
	return &ClickHouseService{Conn: conn}, nil
}

func (s *ClickHouseService) GetTables(ctx context.Context) ([]models.Table, error) {
	rows, err := s.Conn.Query(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %v", err)
	}
	defer rows.Close()

	var tables []models.Table
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, models.Table{Name: tableName})
	}
	return tables, nil
}

func (s *ClickHouseService) GetColumns(ctx context.Context, tableName string) ([]models.Column, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	rows, err := s.Conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch columns: %v", err)
	}
	defer rows.Close()

	var columns []models.Column
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, models.Column{Name: name, Type: typ})
	}
	return columns, nil
}
