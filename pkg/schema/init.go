package schema

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/brainwavecollective/stone-os/pkg/database"
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
	defer func() {
		if tx.IsActive() {
			tx.Rollback()
		}
	}()

	// For SQLite, enable foreign keys
	if db.GetDatabaseType() == "sqlite" {
		_, err := tx.Execute("PRAGMA foreign_keys = ON")
		if err != nil {
			return fmt.Errorf("failed to enable foreign keys: %w", err)
		}
	}

	// Check if schema_version table exists
	var schemaVersionExists bool
	
	if db.GetDatabaseType() == "sqlite" {
		rows, err := tx.ExecuteQuery(`SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'`)
		if err != nil {
			return fmt.Errorf("failed to check for schema_version table: %w", err)
		}
		defer rows.Close()
		
		schemaVersionExists = rows.Next()
	} else {
		rows, err := tx.ExecuteQuery(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables 
				WHERE table_name = 'schema_version'
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to check for schema_version table: %w", err)
		}
		defer rows.Close()
		
		if rows.Next() {
			rows.Scan(&schemaVersionExists)
		}
	}

	// If schema_version doesn't exist, create it and initialize the database
	if !schemaVersionExists {
		fmt.Println("Initializing database schema...")
		
		// Create schema_version table
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
		
		// Apply initial schema
		if err := applyInitialSchema(tx); err != nil {
			return fmt.Errorf("failed to apply initial schema: %w", err)
		}
		
		// Record schema version
		_, err = tx.Execute(`
			INSERT INTO schema_version (version, applied_at, description)
			VALUES (?, ?, ?)
		`, 1, time.Now(), "Initial schema")
		if err != nil {
			return fmt.Errorf("failed to record schema version: %w", err)
		}
	} else {
		// Check current schema version
		rows, err := tx.ExecuteQuery(`SELECT MAX(version) FROM schema_version`)
		if err != nil {
			return fmt.Errorf("failed to get schema version: %w", err)
		}
		defer rows.Close()
		
		var currentVersion int
		if rows.Next() {
			if err := rows.Scan(&currentVersion); err != nil {
				return fmt.Errorf("failed to scan schema version: %w", err)
			}
		}
		
		// Apply any missing migrations
		if currentVersion < CurrentSchemaVersion {
			fmt.Printf("Upgrading schema from version %d to %d...\n", currentVersion, CurrentSchemaVersion)
			
			for version := currentVersion + 1; version <= CurrentSchemaVersion; version++ {
				fmt.Printf("Applying migration to version %d...\n", version)
				
				if err := applyMigration(tx, version); err != nil {
					return fmt.Errorf("failed to apply migration to version %d: %w", version, err)
				}
				
				// Record migration
				_, err = tx.Execute(`
					INSERT INTO schema_version (version, applied_at, description)
					VALUES (?, ?, ?)
				`, version, time.Now(), getMigrationDescription(version))
				if err != nil {
					return fmt.Errorf("failed to record migration to version %d: %w", version, err)
				}
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema initialization: %w", err)
	}
	
	fmt.Println("Database schema initialized successfully.")
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
			path TEXT NOT NULL,
			content BLOB,
			metadata TEXT,
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
			affected_resources TEXT
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
			name TEXT NOT NULL UNIQUE,
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
			is_active BOOLEAN NOT NULL DEFAULT 1,
			is_admin BOOLEAN NOT NULL DEFAULT 0
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Create indexes
	indexStmts := []string{
		"CREATE INDEX idx_resources_parent_id ON resources(parent_id)",
		"CREATE INDEX idx_resources_path ON resources(path)",
		"CREATE INDEX idx_resources_valid_time ON resources(valid_from, valid_to)",
		"CREATE INDEX idx_operations_transaction_id ON operations(transaction_id)",
		"CREATE INDEX idx_transactions_branch_id ON transactions(branch_id)",
	}

	for _, stmt := range indexStmts {
		_, err = tx.Execute(stmt)
		if err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
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
	
	// Create system user
	passwordHash := "system" // In a real system, this would be properly hashed
	_, err = tx.Execute(`
		INSERT INTO users (id, username, password, full_name, created_at, updated_at, is_active, is_admin)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "system", "system", passwordHash, "System User", now, now, true, true)
	if err != nil {
		return fmt.Errorf("failed to create system user: %w", err)
	}

	// Create root directory
	rootID := "root"
	rootMetadata, err := json.Marshal(NewDirectoryMetadata("system"))
	if err != nil {
		return fmt.Errorf("failed to marshal root directory metadata: %w", err)
	}

	_, err = tx.Execute(`
		INSERT INTO resources (id, type, name, parent_id, path, metadata, valid_from, transaction_id)
		VALUES (?, ?, ?, NULL, ?, ?, ?, ?)
	`, rootID, ResourceTypeDirectory, "/", "/", string(rootMetadata), now, "init")
	if err != nil {
		return fmt.Errorf("failed to create root directory: %w", err)
	}

	// Create some standard directories
	standardDirs := []string{"home", "tmp", "usr"}
	for _, dir := range standardDirs {
		dirID := fmt.Sprintf("dir-%s", dir)
		dirPath := fmt.Sprintf("/%s", dir)
		dirMetadata, err := json.Marshal(NewDirectoryMetadata("system"))
		if err != nil {
			return fmt.Errorf("failed to marshal directory metadata: %w", err)
		}

		_, err = tx.Execute(`
			INSERT INTO resources (id, type, name, parent_id, path, metadata, valid_from, transaction_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, dirID, ResourceTypeDirectory, dir, rootID, dirPath, string(dirMetadata), now, "init")
		if err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
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