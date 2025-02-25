package filesystem

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/yourusername/dbos/pkg/database"
	"github.com/yourusername/dbos/pkg/schema"
)

// File represents a file in the filesystem
type File struct {
	ID           string
	Name         string
	ParentID     string
	Path         string
	Content      []byte
	Metadata     schema.ResourceMetadata
	CreatedAt    time.Time
	ModifiedAt   time.Time
	TransactionID string
}

// FileManager handles file operations
type FileManager struct {
	db *database.Connection
}

// NewFileManager creates a new FileManager
func NewFileManager(db *database.Connection) *FileManager {
	return &FileManager{db: db}
}

// GetFile retrieves a file by path
func (fm *FileManager) GetFile(path string, tx *database.Transaction, options database.QueryOptions) (*File, error) {
	// Normalize path
	path = filepath.Clean(path)

	// Query for the file
	var query string
	var result *database.QueryResult
	var err error

	query = `
		SELECT r.id, r.name, r.parent_id, r.content, r.metadata, r.valid_from, r.transaction_id
		FROM resources r
		WHERE r.type = 'file' AND r.path = $1
	`

	if !options.IncludeDeleted {
		query += " AND r.valid_to IS NULL"
	}

	if tx != nil {
		result, err = tx.Query(query, options, path)
	} else {
		result, err = fm.db.Query(query, options, path)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query for file: %w", err)
	}

	if result.Count == 0 {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Parse the result
	row := result.Rows[0]
	
	id := row[0].(string)
	name := row[1].(string)
	parentID := row[2].(string)
	content := row[3].([]byte)
	metadataJSON := row[4].([]byte)
	validFrom := row[5].(time.Time)
	transactionID := row[6].(string)

	var metadata schema.ResourceMetadata
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	file := &File{
		ID:           id,
		Name:         name,
		ParentID:     parentID,
		Path:         path,
		Content:      content,
		Metadata:     metadata,
		CreatedAt:    metadata.CreatedAt,
		ModifiedAt:   metadata.ModifiedAt,
		TransactionID: transactionID,
	}

	return file, nil
}

// CreateFile creates a new file
func (fm *FileManager) CreateFile(path string, content []byte, tx *database.Transaction, owner string) (*File, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction required for file creation")
	}

	// Normalize path
	path = filepath.Clean(path)

	// Parse the path
	dir, name := filepath.Split(path)
	dir = filepath.Clean(dir)

	// Get parent directory ID
	parentID, err := fm.getDirectoryID(dir, tx, database.DefaultQueryOptions())
	if err != nil {
		return nil, fmt.Errorf("parent directory not found: %w", err)
	}

	// Check if file already exists
	exists, err := fm.resourceExists(name, parentID, tx, database.DefaultQueryOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to check if file exists: %w", err)
	}

	if exists {
		return nil, fmt.Errorf("file already exists: %s", path)
	}

	// Create metadata
	metadata := schema.NewResourceMetadata(owner)
	metadata.Size = int64(len(content))
	
	// Detect MIME type (simplified)
	if strings.HasSuffix(name, ".txt") {
		metadata.MimeType = "text/plain"
	} else if strings.HasSuffix(name, ".json") {
		metadata.MimeType = "application/json"
	} else if strings.HasSuffix(name, ".html") {
		metadata.MimeType = "text/html"
	} else {
		metadata.MimeType = "application/octet-stream"
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Generate ID
	id := generateResourceID()

	// Insert the file
	now := time.Now()
	_, err = tx.Execute(`
		INSERT INTO resources (id, type, name, parent_id, path, content, metadata, valid_from, transaction_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, id, schema.ResourceTypeFile, name, parentID, path, content, metadataJSON, now, tx.GetID())

	if err != nil {
		return nil, fmt.Errorf("failed to insert file: %w", err)
	}

	file := &File{
		ID:           id,
		Name:         name,
		ParentID:     parentID,
		Path:         path,
		Content:      content,
		Metadata:     metadata,
		CreatedAt:    now,
		ModifiedAt:   now,
		TransactionID: tx.GetID(),
	}

	return file, nil
}

// UpdateFile updates an existing file
func (fm *FileManager) UpdateFile(path string, content []byte, tx *database.Transaction) (*File, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction required for file update")
	}

	// Normalize path
	path = filepath.Clean(path)

	// Get the current file
	options := database.DefaultQueryOptions()
	file, err := fm.GetFile(path, tx, options)
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}

	// Mark the old version as invalid
	now := time.Now()
	_, err = tx.Execute(`
		UPDATE resources
		SET valid_to = $1
		WHERE id = $2 AND valid_to IS NULL
	`, now, file.ID)

	if err != nil {
		return nil, fmt.Errorf("failed to mark old file version as invalid: %w", err)
	}

	// Update metadata
	file.Metadata.ModifiedAt = now
	file.Metadata.Size = int64(len(content))
	
	metadataJSON, err := json.Marshal(file.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Insert the new version
	newID := generateResourceID()
	_, err = tx.Execute(`
		INSERT INTO resources (id, type, name, parent_id, path, content, metadata, valid_from, transaction_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, newID, schema.ResourceTypeFile, file.Name, file.ParentID, path, content, metadataJSON, now, tx.GetID())

	if err != nil {
		return nil, fmt.Errorf("failed to insert new file version: %w", err)
	}

	updatedFile := &File{
		ID:           newID,
		Name:         file.Name,
		ParentID:     file.ParentID,
		Path:         path,
		Content:      content,
		Metadata:     file.Metadata,
		CreatedAt:    file.CreatedAt,
		ModifiedAt:   now,
		TransactionID: tx.GetID(),
	}

	return updatedFile, nil
}

// DeleteFile marks a file as deleted
func (fm *FileManager) DeleteFile(path string, tx *database.Transaction) error {
	if tx == nil {
		return fmt.Errorf("transaction required for file deletion")
	}

	// Normalize path
	path = filepath.Clean(path)

	// Get the current file
	options := database.DefaultQueryOptions()
	file, err := fm.GetFile(path, tx, options)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	// Mark the file as deleted
	now := time.Now()
	_, err = tx.Execute(`
		UPDATE resources
		SET valid_to = $1
		WHERE id = $2 AND valid_to IS NULL
	`, now, file.ID)

	if err != nil {
		return fmt.Errorf("failed to mark file as deleted: %w", err)
	}

	return nil
}

// getDirectoryID gets the ID of a directory by path
func (fm *FileManager) getDirectoryID(path string, tx *database.Transaction, options database.QueryOptions) (string, error) {
	// Special case for root directory
	if path == "/" {
		return "root", nil
	}

	// Normalize path
	path = filepath.Clean(path)

	// Query for the directory
	var query string
	var result *database.QueryResult
	var err error

	query = `
		SELECT id
		FROM resources
		WHERE type = 'directory' AND path = $1
	`

	if !options.IncludeDeleted {
		query += " AND valid_to IS NULL"
	}

	if tx != nil {
		result, err = tx.Query(query, options, path)
	} else {
		result, err = fm.db.Query(query, options, path)
	}

	if err != nil {
		return "", fmt.Errorf("failed to query for directory: %w", err)
	}

	if result.Count == 0 {
		return "", fmt.Errorf("directory not found: %s", path)
	}

	return result.Rows[0][0].(string), nil
}

// resourceExists checks if a resource with the given name exists in the given parent directory
func (fm *FileManager) resourceExists(name string, parentID string, tx *database.Transaction, options database.QueryOptions) (bool, error) {
	var query string
	var result *database.QueryResult
	var err error

	query = `
		SELECT 1
		FROM resources
		WHERE name = $1 AND parent_id = $2
	`

	if !options.IncludeDeleted {
		query += " AND valid_to IS NULL"
	}

	if tx != nil {
		result, err = tx.Query(query, options, name, parentID)
	} else {
		result, err = fm.db.Query(query, options, name, parentID)
	}

	if err != nil {
		return false, fmt.Errorf("failed to check if resource exists: %w", err)
	}

	return result.Count > 0, nil
}

// generateResourceID generates a unique resource ID
func generateResourceID() string {
	return fmt.Sprintf("r-%d", time.Now().UnixNano())
}