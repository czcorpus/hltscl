package hltscl

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pgDefaultPort = 5432
	pgDefaultHost = "localhost"
)

type PgConf struct {
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	DBName string `json:"dbName"`
}

// CreateConnString produces Postgres connection
// string with the form:
// "postgres://username:password@host:port/dbname"
func (conf PgConf) CreateConnString() string {
	port := conf.Port
	if port == 0 {
		port = pgDefaultPort
	}
	host := conf.Host
	if host == "" {
		host = pgDefaultHost
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		conf.User, conf.Passwd, host, port, conf.DBName,
	)
}

func CreatePool(conf PgConf) (*pgxpool.Pool, error) {
	ctx := context.Background()
	return pgxpool.New(ctx, conf.CreateConnString())
}
