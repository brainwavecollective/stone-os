package database

import (
	"database/sql"
	"fmt"
	"time"
)

// TransactionStatus represents the current status of a transaction
type TransactionStatus string

const (
	TransactionStatusActive    TransactionStatus = "active"
	TransactionStatusCommitted TransactionStatus = "committed"
	TransactionStatusRolledBack TransactionStatus = "rolled_back"
)

// Transaction represents a database transaction
type Transaction struct {
	tx         *sql.Tx
	id         string
	startTime  time.Time
	endTime    time.Time
	status     TransactionStatus
	savepoints map[string]time.Time
	connection *Connection
	branchID   string
	userID     string
}

// Execute executes a SQL statement within the transaction
func (t *Transaction) Execute(statement string, args ...interface{}) (sql.Result, error) {
	if t.status != TransactionStatusActive {
		return nil, fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	return t.tx.Exec(statement, args...)
}

// ExecuteQuery executes a SQL query within the transaction
func (t *Transaction) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	if t.status != TransactionStatusActive {
		return nil, fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	return t.tx.Query(query, args...)
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	if t.status != TransactionStatusActive {
		return fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	err := t.tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	
	t.status = TransactionStatusCommitted
	t.endTime = time.Now()
	
	// Record the committed transaction in the database
	// This would normally involve a separate connection to the database
	// for recording metadata about the transaction
	
	return nil
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	if t.status != TransactionStatusActive {
		return fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	err := t.tx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to roll back transaction: %w", err)
	}
	
	t.status = TransactionStatusRolledBack
	t.endTime = time.Now()
	
	return nil
}

// Savepoint creates a savepoint within the transaction
func (t *Transaction) Savepoint(name string) error {
	if t.status != TransactionStatusActive {
		return fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	if t.savepoints == nil {
		t.savepoints = make(map[string]time.Time)
	}
	
	// Create savepoint in the database
	_, err := t.tx.Exec(fmt.Sprintf("SAVEPOINT %s", name))
	if err != nil {
		return fmt.Errorf("failed to create savepoint: %w", err)
	}
	
	t.savepoints[name] = time.Now()
	return nil
}

// RollbackToSavepoint rolls back to a savepoint within the transaction
func (t *Transaction) RollbackToSavepoint(name string) error {
	if t.status != TransactionStatusActive {
		return fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	if t.savepoints == nil || t.savepoints[name].IsZero() {
		return fmt.Errorf("savepoint '%s' does not exist", name)
	}
	
	// Roll back to savepoint in the database
	_, err := t.tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name))
	if err != nil {
		return fmt.Errorf("failed to roll back to savepoint: %w", err)
	}
	
	return nil
}

// ReleaseSavepoint releases a savepoint within the transaction
func (t *Transaction) ReleaseSavepoint(name string) error {
	if t.status != TransactionStatusActive {
		return fmt.Errorf("transaction is not active (status: %s)", t.status)
	}
	
	if t.savepoints == nil || t.savepoints[name].IsZero() {
		return fmt.Errorf("savepoint '%s' does not exist", name)
	}
	
	// Release savepoint in the database
	_, err := t.tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", name))
	if err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}
	
	delete(t.savepoints, name)
	return nil
}

// IsActive returns whether the transaction is active
func (t *Transaction) IsActive() bool {
	return t.status == TransactionStatusActive
}

// GetID returns the transaction ID
func (t *Transaction) GetID() string {
	return t.id
}

// GetStartTime returns the transaction start time
func (t *Transaction) GetStartTime() time.Time {
	return t.startTime
}

// GetEndTime returns the transaction end time
func (t *Transaction) GetEndTime() time.Time {
	return t.endTime
}

// GetStatus returns the transaction status
func (t *Transaction) GetStatus() TransactionStatus {
	return t.status
}

// SetBranchID sets the branch ID for the transaction
func (t *Transaction) SetBranchID(branchID string) {
	t.branchID = branchID
}

// GetBranchID gets the branch ID for the transaction
func (t *Transaction) GetBranchID() string {
	return t.branchID
}

// SetUserID sets the user ID for the transaction
func (t *Transaction) SetUserID(userID string) {
	t.userID = userID
}

// GetUserID gets the user ID for the transaction
func (t *Transaction) GetUserID() string {
	return t.userID
}