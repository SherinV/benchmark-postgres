package dbclient

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"

	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

const maxConnections = 8

var pool *pgxpool.Pool
var InsertChan chan *Record

type Record struct {
	UID        string
	Cluster    string
	Name       string
	Properties map[string]interface{}
	EdgesTo    string
	EdgesFrom  string
}

func init() {
	InsertChan = make(chan *Record, 100)
	createPool()

	// Start go routines to process insert.
	go batchInsert("A")
	go batchInsert("B")
}

// Initializes the connection pool.
func createPool() {
	DB_HOST := getEnvOrUseDefault("DB_HOST", "localhost")
	DB_USER := getEnvOrUseDefault("DB_USER", "hippo")
	DB_NAME := getEnvOrUseDefault("DB_NAME", "hippo")
	DB_PASSWORD := url.QueryEscape(getEnvOrUseDefault("DB_PASSWORD", ">Z]verCX9X@opiR{uC>C39HH"))
	DB_PORT, err := strconv.Atoi(getEnvOrUseDefault("DB_PORT", "5432"))
	if err != nil {
		DB_PORT = 5432
		fmt.Println("Error parsing db port, using default 5432")
	}

	database_url := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
	//database_url := "postgresql://hippo:VR%3FG7Jrdz%29OjJki%28%5Dl0.FAR%7B@localhost:5432/hippo"

	fmt.Println("Connecting to PostgreSQL at: ", database_url)
	config, connerr := pgxpool.ParseConfig(database_url)
	if connerr != nil {
		fmt.Println("Error connecting to DB:", connerr)
	}
	config.MaxConns = maxConnections
	conn, err := pgxpool.ConnectConfig(context.Background(), config)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	pool = conn
}

func GetConnection() *pgxpool.Pool {
	err := pool.Ping(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected!")
	return pool
}

func getEnvOrUseDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
