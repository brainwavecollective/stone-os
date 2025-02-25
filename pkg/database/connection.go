package database

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"           // PostgreSQL driver
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Connection represents a database connection
type Connection struct {
	db           *sql.DB
	dbType       string
	connectionID string
	mu           sync.Mutex
	txs          map[string]*Transaction
}

// ConnectionConfig holds database connection configuration
type ConnectionConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// DefaultConfig returns a default connection configuration
func DefaultConfig() ConnectionConfig {
	return ConnectionConfig{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}
}

// Connect establishes a connection to the specified database
func Connect(dbType, connString string) (*Connection, error) {
	return ConnectWithConfig(dbType, connString, DefaultConfig())
}

// ConnectWithConfig establishes a connection with custom configuration
func ConnectWithConfig(dbType, connString string, config ConnectionConfig) (*Connection, error) {
	var driverName string
	
	switch dbType {
	case "sqlite":
		driverName = "sqlite3"
	case "postgres":
		driverName = "postgres"
	case "inmemory":
		driverName = "sqlite3"
		connString = ":memory:"
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
	
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	
	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	
	conn := &Connection{
		db:           db,
		dbType:       dbType,
		connectionID: GenerateUUID(),
		txs:          make(map[string]*Transaction),
	}
	
	return conn, nil
}

// Close closes the database connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Roll back any active transactions
	for id, tx := range c.txs {
		if tx.IsActive() {
			tx.Rollback()
		}
		delete(c.txs, id)
	}
	
	return c.db.Close()
}

// Begin starts a new transaction
func (c *Connection) Begin() (*Transaction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	tx, err := c.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	
	transaction := &Transaction{
		tx:         tx,
		id:         GenerateUUID(),
		startTime:  time.Now(),
		status:     TransactionStatusActive,
		connection: c,
	}
	
	c.txs[transaction.id] = transaction
	
	return transaction, nil
}

// ExecuteQuery executes a SQL query without a transaction
func (c *Connection) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.Query(query, args...)
}

// ExecuteStatement executes a SQL statement without a transaction
func (c *Connection) ExecuteStatement(statement string, args ...interface{}) (sql.Result, error) {
	return c.db.Exec(statement, args...)
}

// GetDatabaseType returns the type of database being used
func (c *Connection) GetDatabaseType() string {
	return c.dbType
}

// GetConnectionID returns the unique ID for this connection
func (c *Connection) GetConnectionID() string {
	return c.connectionID
}

// GetActiveTransactionCount returns the number of active transactions
func (c *Connection) GetActiveTransactionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	count := 0
	for _, tx := range c.txs {
		if tx.IsActive() {
			count++
		}
	}
	
	return count
}

// GenerateUUID generates a new UUID string
func GenerateUUID() string {
	// Simple UUID generation for now
	// In a real implementation, use a proper UUID library
	return fmt.Sprintf("%d", time.Now().UnixNano())
}