package schema

import (
	"encoding/json"
	"time"
)

// Resource represents a system resource (file, directory, etc.)
type Resource struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`          // "file", "directory", "symlink", etc.
	Name          string          `json:"name"`
	ParentID      string          `json:"parent_id"`
	Content       []byte          `json:"content,omitempty"`
	Metadata      json.RawMessage `json:"metadata"`
	ValidFrom     time.Time       `json:"valid_from"`
	ValidTo       *time.Time      `json:"valid_to"`      // NULL means currently valid
	TransactionID string          `json:"transaction_id"`
}

// ResourceMetadata represents metadata for a resource
type ResourceMetadata struct {
	Permissions  uint32    `json:"permissions"`
	Owner        string    `json:"owner"`
	Group        string    `json:"group"`
	CreatedAt    time.Time `json:"created_at"`
	ModifiedAt   time.Time `json:"modified_at"`
	AccessedAt   time.Time `json:"accessed_at"`
	Size         int64     `json:"size"`
	MimeType     string    `json:"mime_type,omitempty"`
	IsExecutable bool      `json:"is_executable"`
	IsHidden     bool      `json:"is_hidden"`
	IsSystem     bool      `json:"is_system"`
	Checksum     string    `json:"checksum,omitempty"`
	SymlinkTarget string    `json:"symlink_target,omitempty"`
}

// Operation represents a command executed in the system
type Operation struct {
	ID                string          `json:"id"`
	UserID            string          `json:"user_id"`
	CommandText       string          `json:"command_text"`
	Timestamp         time.Time       `json:"timestamp"`
	TransactionID     string          `json:"transaction_id"`
	AffectedResources json.RawMessage `json:"affected_resources"` // IDs of modified resources
}

// Transaction represents a database transaction
type Transaction struct {
	ID        string     `json:"id"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`
	Status    string     `json:"status"` // "active", "committed", "aborted"
	UserID    string     `json:"user_id"`
	BranchID  string     `json:"branch_id"`
}

// Branch represents a parallel state branch
type Branch struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	BaseStateID string    `json:"base_state_id"` // Point where branch was created
	CreatedAt  time.Time `json:"created_at"`
	CreatedBy  string    `json:"created_by"`
	Status     string    `json:"status"` // "active", "merged", "abandoned"
}

// User represents a system user
type User struct {
	ID       string    `json:"id"`
	Username string    `json:"username"`
	Password string    `json:"password"` // Hashed
	FullName string    `json:"full_name"`
	Email    string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	LastLogin *time.Time `json:"last_login"`
	IsActive  bool      `json:"is_active"`
	IsAdmin   bool      `json:"is_admin"`
}

// ResourceType constants
const (
	ResourceTypeFile      = "file"
	ResourceTypeDirectory = "directory"
	ResourceTypeSymlink   = "symlink"
)

// TransactionStatus constants
const (
	TransactionStatusActive    = "active"
	TransactionStatusCommitted = "committed"
	TransactionStatusAborted   = "aborted"
)

// BranchStatus constants
const (
	BranchStatusActive    = "active"
	BranchStatusMerged    = "merged"
	BranchStatusAbandoned = "abandoned"
)

// NewResourceMetadata creates a new ResourceMetadata with default values
func NewResourceMetadata(owner string) ResourceMetadata {
	now := time.Now()
	
	return ResourceMetadata{
		Permissions: 0644, // Default file permissions (rw-r--r--)
		Owner:       owner,
		Group:       "users",
		CreatedAt:   now,
		ModifiedAt:  now,
		AccessedAt:  now,
		IsExecutable: false,
		IsHidden:    false,
		IsSystem:    false,
	}
}

// NewDirectoryMetadata creates metadata for a new directory
func NewDirectoryMetadata(owner string) ResourceMetadata {
	metadata := NewResourceMetadata(owner)
	metadata.Permissions = 0755 // Default directory permissions (rwxr-xr-x)
	return metadata
}