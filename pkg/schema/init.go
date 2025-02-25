package schema

import (
	"fmt"
	"time"

	"github.com/yourusername/dbos/pkg/database"
)

// SchemaVersion represents the version of the database schema
type SchemaVersion struct {
	Version     int       `json:"version"`
	AppliedAt   time.Time `json:"applied_at"`
	Description string    `json:"description"`
}

// CurrentSchemaVersion is the current version of the schema
const CurrentSchemaVersion = 1

// Initialize initializes the database schema
func Initialize(db *database.Connection) error {
	// Start a transaction for schema initialization
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for schema initialization: %w", err)
	}
	defer tx.Rollback()

	// Check if schema_version table exists
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_name = 'schema_version'
		)
	`

	// For SQLite, use a different query
	if db.GetDatabaseType() == "sqlite" {
		query = `
			SELECT EXISTS (
				SELECT 1
				FROM sqlite_master
				WHERE type='table' AND name='schema_version'
			)
		`
	}

	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to check if schema_version table exists: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&exists); err != nil {
			return fmt.Errorf("failed to scan schema_version existence: %w", err)
		}
	}

	// Create schema_version table if it doesn't exist
	if !exists {
		_, err := tx.Execute(`
			CREATE TABLE schema_version (
				version INTEGER PRIMARY KEY,
				applied_at TIMESTAMP NOT NULL,
				description TEXT NOT NULL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to create schema_version table: %w", err)
		}
	}

	// Get current schema version
	var currentVersion int
	if exists {
		row := tx.tx.QueryRow("SELECT MAX(version) FROM schema_version")
		if err := row.Scan(&currentVersion); err != nil {
			return fmt.Errorf("failed to get current schema version: %w", err)
		}
	}

	// Apply migrations if needed
	if currentVersion < CurrentSchemaVersion {
		if err := applyMigrations(tx, currentVersion, CurrentSchemaVersion); err != nil {
			return fmt.Errorf("failed to apply migrations: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema initialization: %w", err)
	}

	return nil
}

// applyMigrations applies database migrations from start version to end version
func applyMigrations(tx *database.Transaction, startVersion, endVersion int) error {
	for version := startVersion + 1; version <= endVersion; version++ {
		fmt.Printf("Applying migration to version %d...\n", version)
		
		// Apply migration
		if err := applyMigration(tx, version); err != nil {
			return fmt.Errorf("failed to apply migration to version %d: %w", version, err)
		}
		
		// Record migration
		_, err := tx.Execute(
			"INSERT INTO schema_version (version, applied_at, description) VALUES (?, ?, ?)",
			version,
			time.Now(),
			getMigrationDescription(version),
		)
		if err != nil {
			return fmt.Errorf("failed to record migration to version %d: %w", version, err)
		}
	}
	
	return nil
}

// applyMigration applies a specific migration
func applyMigration(tx *database.Transaction, version int) error {
	switch version {
	case 1:
		return applyInitialSchema(tx)
	default:
		return fmt.Errorf("unknown schema version: %d", version)
	}
}

// getMigrationDescription returns the description for a migration
func getMigrationDescription(version int) string {
	switch version {
	case 1:
		return "Initial schema"
	default:
		return fmt.Sprintf("Migration to version %d", version)
	}
}

// applyInitialSchema creates the initial database schema
func applyInitialSchema(tx *database.Transaction) error {
	// Create resources table
	_, err := tx.Execute(`
		CREATE TABLE resources (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			name TEXT NOT NULL,
			parent_id TEXT REFERENCES resources(id),
			content BLOB,
			metadata JSONB,
			valid_from TIMESTAMP NOT NULL,
			valid_to TIMESTAMP,
			transaction_id TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create resources table: %w", err)
	}

	// Create operations table
	_, err = tx.Execute(`
		CREATE TABLE operations (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			command_text TEXT NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			transaction_id TEXT NOT NULL,
			affected_resources JSONB
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create operations table: %w", err)
	}

	// Create transactions table
	_, err = tx.Execute(`
		CREATE TABLE transactions (
			id TEXT PRIMARY KEY,
			start_time TIMESTAMP NOT NULL,
			end_time TIMESTAMP,
			status TEXT NOT NULL,
			user_id TEXT NOT NULL,
			branch_id TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	// Create branches table
	_, err = tx.Execute(`
		CREATE TABLE branches (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			base_state_id TEXT,
			created_at TIMESTAMP NOT NULL,
			created_by TEXT NOT NULL,
			status TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create branches table: %w", err)
	}

	// Create users table
	_, err = tx.Execute(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			username TEXT NOT NULL UNIQUE,
			password TEXT NOT NULL,
			full_name TEXT,
			email TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			last_login TIMESTAMP,
			is_active BOOLEAN NOT NULL,
			is_admin BOOLEAN NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Create indexes
	_, err = tx.Execute("CREATE INDEX idx_resources_parent_id ON resources(parent_id)")
	if err != nil {
		return fmt.Errorf("failed to create index on resources.parent_id: %w", err)
	}

	_, err = tx.Execute("CREATE INDEX idx_resources_valid_time ON resources(valid_from, valid_to)")
	if err != nil {
		return fmt.Errorf("failed to create index on resources.valid_time: %w", err)
	}

	_, err = tx.Execute("CREATE INDEX idx_operations_transaction_id ON operations(transaction_id)")
	if err != nil {
		return fmt.Errorf("failed to create index on operations.transaction_id: %w", err)
	}

	_, err = tx.Execute("CREATE INDEX idx_transactions_branch_id ON transactions(branch_id)")
	if err != nil {
		return fmt.Errorf("failed to create index on transactions.branch_id: %w", err)
	}

	// Create default branch
	now := time.Now()
	_, err = tx.Execute(`
		INSERT INTO branches (id, name, created_at, created_by, status)
		VALUES (?, ?, ?, ?, ?)
	`, "main", "main", now, "system", "active")
	if err != nil {
		return fmt.Errorf("failed to create default branch: %w", err)
	}

	// Create root directory
	rootID := "root"
	rootMetadata := NewDirectoryMetadata("system")
	metadataBytes, err := json.Marshal(rootMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal root directory metadata: %w", err)
	}

	_, err = tx.Execute(`
		INSERT INTO resources (id, type, name, parent_id, metadata, valid_from, transaction_id)
		VALUES (?, ?, ?, NULL, ?, ?, ?)
	`, rootID, ResourceTypeDirectory, "/", metadataBytes, now, "init")
	if err != nil {
		return fmt.Errorf("failed to create root directory: %w", err)
	}

	// Create system transaction record
	_, err = tx.Execute(`
		INSERT INTO transactions (id, start_time, end_time, status, user_id, branch_id)
		VALUES (?, ?, ?, ?, ?, ?)
	`, "init", now, now, TransactionStatusCommitted, "system", "main")
	if err != nil {
		return fmt.Errorf("failed to create system transaction: %w", err)
	}

	return nil
}