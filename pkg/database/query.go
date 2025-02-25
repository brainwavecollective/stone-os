package database

import (
	"database/sql"
	"fmt"
	"time"
)

// QueryOptions contains options for customizing queries
type QueryOptions struct {
	PointInTime       *time.Time // For time-travel queries
	BranchID          string     // For branch-specific queries
	IncludeDeleted    bool       // Whether to include deleted resources
	Limit             int        // Limit the number of results
	Offset            int        // Offset for pagination
	OrderBy           string     // Column to order by
	OrderDirection    string     // "ASC" or "DESC"
	TemporalCondition string     // "AS OF", "FROM", "BETWEEN", etc.
}

// DefaultQueryOptions returns default query options
func DefaultQueryOptions() QueryOptions {
	return QueryOptions{
		PointInTime:       nil,
		BranchID:          "main",
		IncludeDeleted:    false,
		Limit:             0,
		Offset:            0,
		OrderBy:           "name",
		OrderDirection:    "ASC",
		TemporalCondition: "AS OF",
	}
}

// QueryResult represents the result of a query
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
	Count   int
}

// Query executes a custom SQL query with the given options
func (c *Connection) Query(query string, options QueryOptions, args ...interface{}) (*QueryResult, error) {
	// Apply options to query
	query = applyQueryOptions(query, options)
	
	rows, err := c.ExecuteQuery(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()
	
	return processQueryRows(rows)
}

// QueryWithTransaction executes a query within a transaction
func (tx *Transaction) Query(query string, options QueryOptions, args ...interface{}) (*QueryResult, error) {
	// Apply options to query
	query = applyQueryOptions(query, options)
	
	rows, err := tx.tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed within transaction: %w", err)
	}
	defer rows.Close()
	
	return processQueryRows(rows)
}

// FindResources finds resources matching the given criteria
func (c *Connection) FindResources(parentID string, resourceType string, options QueryOptions) (*QueryResult, error) {
	query := `
		SELECT id, type, name, parent_id, content, metadata, valid_from, valid_to, transaction_id
		FROM resources
		WHERE 1=1
	`
	
	args := []interface{}{}
	argIndex := 1
	
	if parentID != "" {
		query += fmt.Sprintf(" AND parent_id = $%d", argIndex)
		args = append(args, parentID)
		argIndex++
	}
	
	if resourceType != "" {
		query += fmt.Sprintf(" AND type = $%d", argIndex)
		args = append(args, resourceType)
		argIndex++
	}
	
	if !options.IncludeDeleted {
		query += " AND valid_to IS NULL"
	}
	
	return c.Query(query, options, args...)
}

// FindResourceByPath finds a resource by its path
func (c *Connection) FindResourceByPath(path string, options QueryOptions) (*QueryResult, error) {
	// This is a simplified implementation
	// In a real system, this would involve parsing the path and traversing the hierarchy
	
	query := `
		SELECT id, type, name, parent_id, content, metadata, valid_from, valid_to, transaction_id
		FROM resources
		WHERE path = $1
	`
	
	if !options.IncludeDeleted {
		query += " AND valid_to IS NULL"
	}
	
	return c.Query(query, options, path)
}

// GetResourceHistory gets the history of changes to a resource
func (c *Connection) GetResourceHistory(resourceID string, options QueryOptions) (*QueryResult, error) {
	query := `
		SELECT r.id, r.type, r.name, r.parent_id, r.metadata, r.valid_from, r.valid_to, 
		       t.id as transaction_id, t.start_time, t.end_time, t.status, t.user_id
		FROM resources r
		JOIN transactions t ON r.transaction_id = t.id
		WHERE r.id = $1
		ORDER BY r.valid_from DESC
	`
	
	return c.Query(query, options, resourceID)
}

// applyQueryOptions applies query options to a SQL query
func applyQueryOptions(query string, options QueryOptions) string {
	// This is a simplified implementation
	// In a real system, this would involve more complex SQL generation
	
	// Apply temporal condition if a point in time is specified
	if options.PointInTime != nil {
		// Example for PostgreSQL's temporal queries
		if options.TemporalCondition == "AS OF" {
			query += fmt.Sprintf(" AS OF SYSTEM TIME '%s'", options.PointInTime.Format(time.RFC3339))
		}
	}
	
	// Apply branch condition
	if options.BranchID != "" {
		// This is simplified; in a real system, this would be more complex
		query += fmt.Sprintf(" AND branch_id = '%s'", options.BranchID)
	}
	
	// Apply order by
	if options.OrderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", options.OrderBy, options.OrderDirection)
	}
	
	// Apply limit and offset
	if options.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", options.Limit)
		
		if options.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", options.Offset)
		}
	}
	
	return query
}

// processQueryRows processes SQL rows into a QueryResult
func processQueryRows(rows *sql.Rows) (*QueryResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	
	result := &QueryResult{
		Columns: columns,
		Rows:    [][]interface{}{},
		Count:   0,
	}
	
	for rows.Next() {
		// Create a slice of interface{} to hold the row values
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))
		
		// Create pointers to each element in the values slice
		for i := range values {
			valuePointers[i] = &values[i]
		}
		
		// Scan the row into the valuePointers
		if err := rows.Scan(valuePointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		result.Rows = append(result.Rows, values)
		result.Count++
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return result, nil
}