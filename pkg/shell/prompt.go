package shell

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/brainwavecollective/stone-os/pkg/database"
	"github.com/brainwavecollective/stone-os/pkg/schema"
)

// formatSize formats a size in bytes to a human-readable string
func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// ShellState represents the current state of the shell
type ShellState struct {
	CurrentTransaction *database.Transaction
	CurrentBranch      string
	CurrentDirectory   string
	User               string
	PointInTime        *time.Time
	IsInteractive      bool
	Verbose            bool
}

// Shell represents the interactive shell
type Shell struct {
	db        *database.Connection
	state     ShellState
	history   []string
	running   bool
	promptFmt string
}

// NewShell creates a new interactive shell
func NewShell(db *database.Connection) *Shell {
	// Default state
	state := ShellState{
		CurrentTransaction: nil,
		CurrentBranch:      "main",
		CurrentDirectory:   "/",
		User:               os.Getenv("USER"),
		PointInTime:        nil,
		IsInteractive:      true,
		Verbose:            true,
	}

	// Default prompt format
	promptFmt := "[%s] %s@%s:%s%s> "

	return &Shell{
		db:        db,
		state:     state,
		history:   []string{},
		running:   false,
		promptFmt: promptFmt,
	}
}

// Run starts the interactive shell
func (s *Shell) Run() {
	s.running = true

	for s.running {
		prompt := s.GetPrompt()
		fmt.Print(prompt)

		// Read a full line of input including spaces
		var input string
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			input = scanner.Text()
		} else {
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Add to history
		s.AddToHistory(input)

		// Process command
		if err := s.ProcessCommand(input); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
	}
}

// GetPrompt returns the shell prompt string
func (s *Shell) GetPrompt() string {
	txIndicator := ""
	if s.state.CurrentTransaction != nil {
		txIndicator = fmt.Sprintf("(T%s)", s.state.CurrentTransaction.GetID()[:8])
	}

	timeIndicator := ""
	if s.state.PointInTime != nil {
		timeIndicator = fmt.Sprintf("@%s", s.state.PointInTime.Format("2006-01-02T15:04:05"))
	}

	return fmt.Sprintf(
		s.promptFmt,
		s.state.CurrentBranch,
		s.state.User,
		timeIndicator,
		s.state.CurrentDirectory,
		txIndicator,
	)
}

// ProcessCommand processes a command string
func (s *Shell) ProcessCommand(cmdStr string) error {
	// Split command and arguments
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]
	args := parts[1:]

	// Handle built-in commands
	switch cmd {
	case "exit", "quit":
		s.running = false
		return nil

	case "help":
		s.ShowHelp()
		return nil

	case "cd":
		return s.ChangeDirectory(args)

	case "ls":
		return s.ListDirectory(args)

	case "mkdir":
		return s.MakeDirectory(args)

	case "touch":
		return s.TouchFile(args)

	case "rm":
		return s.RemoveResource(args)

	case "cat":
		return s.CatFile(args)

	case "echo":
		return s.Echo(args)

	case "begin":
		return s.BeginTransaction()

	case "commit":
		return s.CommitTransaction()

	case "abort", "rollback":
		return s.AbortTransaction()

	case "branch":
		return s.ManageBranch(args)

	case "switch":
		return s.SwitchBranch(args)

	case "history":
		return s.ShowHistory(args)

	case "state-at":
		return s.SetPointInTime(args)

	case "now":
		return s.ResetPointInTime()

	case "query":
		return s.ExecuteQuery(args)

	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

// AddToHistory adds a command to history
func (s *Shell) AddToHistory(cmd string) {
	s.history = append(s.history, cmd)
}

// ShowHelp displays help information
func (s *Shell) ShowHelp() {
	fmt.Println("DBOS CLI Help")
	fmt.Println("=============")
	fmt.Println()
	fmt.Println("File Operations:")
	fmt.Println("  ls [path]                 List directory contents")
	fmt.Println("  cd [path]                 Change current directory")
	fmt.Println("  mkdir <dir>               Create a directory")
	fmt.Println("  touch <file>              Create an empty file")
	fmt.Println("  rm <resource>             Remove a resource")
	fmt.Println("  cat <file>                Display file contents")
	fmt.Println("  echo <text> > <file>      Write text to file")
	fmt.Println()
	fmt.Println("Transaction Management:")
	fmt.Println("  begin                     Start a transaction")
	fmt.Println("  commit                    Commit current transaction")
	fmt.Println("  abort, rollback           Abort current transaction")
	fmt.Println()
	fmt.Println("Branching:")
	fmt.Println("  branch <name>             Create a new branch")
	fmt.Println("  branch                    List branches")
	fmt.Println("  switch <branch>           Switch to a branch")
	fmt.Println()
	fmt.Println("Time Travel:")
	fmt.Println("  state-at <time>           View system at point in time")
	fmt.Println("  now                       Return to present time")
	fmt.Println("  history [resource]        Show history of a resource")
	fmt.Println()
	fmt.Println("Query:")
	fmt.Println("  query <sql>               Execute a SQL query")
	fmt.Println()
	fmt.Println("Shell:")
	fmt.Println("  help                      Show this help")
	fmt.Println("  exit, quit                Exit the shell")
}

// ChangeDirectory changes the current directory
func (s *Shell) ChangeDirectory(args []string) error {
	path := "/"
	if len(args) > 0 {
		path = args[0]
	}

	// Handle relative paths
	if !strings.HasPrefix(path, "/") {
		path = filepath.Join(s.state.CurrentDirectory, path)
	}

	// Special case for "cd .." - go up one directory
	if path == ".." || path == "../" {
		if s.state.CurrentDirectory == "/" {
			return nil // Already at root
		}
		path = filepath.Dir(s.state.CurrentDirectory)
	}

	// Normalize path
	path = filepath.Clean(path)
	if path == "." {
		path = s.state.CurrentDirectory
	}

	// Verify directory exists by querying the database directly
	var query string
	if s.state.PointInTime != nil {
		query = `
			SELECT 1 FROM resources 
			WHERE type = 'directory' AND path = ? AND valid_from <= ? 
			AND (valid_to IS NULL OR valid_to > ?)
		`
	} else {
		query = `
			SELECT 1 FROM resources 
			WHERE type = 'directory' AND path = ? AND valid_to IS NULL
		`
	}
	
	var rows *sql.Rows
	var err error
	
	if s.state.PointInTime != nil {
		pointInTime := *s.state.PointInTime
		if s.state.CurrentTransaction != nil {
			rows, err = s.state.CurrentTransaction.ExecuteQuery(query, path, pointInTime, pointInTime)
		} else {
			rows, err = s.db.ExecuteQuery(query, path, pointInTime, pointInTime)
		}
	} else {
		if s.state.CurrentTransaction != nil {
			rows, err = s.state.CurrentTransaction.ExecuteQuery(query, path)
		} else {
			rows, err = s.db.ExecuteQuery(query, path)
		}
	}
	
	if err != nil {
		return fmt.Errorf("failed to check directory: %w", err)
	}
	defer rows.Close()
	
	var exists bool
	if rows.Next() {
		exists = true
	}
	
	if !exists {
		return fmt.Errorf("directory not found: %s", path)
	}

	// Update current directory
	s.state.CurrentDirectory = path
	return nil
}

// ListDirectory lists the contents of a directory
func (s *Shell) ListDirectory(args []string) error {
	// Determine path to list
	path := s.state.CurrentDirectory
	if len(args) > 0 {
		if strings.HasPrefix(args[0], "/") {
			// Absolute path
			path = args[0]
		} else {
			// Relative path
			path = filepath.Join(s.state.CurrentDirectory, args[0])
		}
	}
	path = filepath.Clean(path)

	// First, verify the directory exists and get its ID
	var query string
	if s.state.PointInTime != nil {
		query = `
			SELECT id FROM resources 
			WHERE type = 'directory' AND path = ? AND valid_from <= ? 
			AND (valid_to IS NULL OR valid_to > ?)
		`
	} else {
		query = `
			SELECT id FROM resources 
			WHERE type = 'directory' AND path = ? AND valid_to IS NULL
		`
	}
	
	var rows *sql.Rows
	var err error
	
	if s.state.PointInTime != nil {
		pointInTime := *s.state.PointInTime
		if s.state.CurrentTransaction != nil {
			rows, err = s.state.CurrentTransaction.ExecuteQuery(query, path, pointInTime, pointInTime)
		} else {
			rows, err = s.db.ExecuteQuery(query, path, pointInTime, pointInTime)
		}
	} else {
		if s.state.CurrentTransaction != nil {
			rows, err = s.state.CurrentTransaction.ExecuteQuery(query, path)
		} else {
			rows, err = s.db.ExecuteQuery(query, path)
		}
	}
	
	if err != nil {
		return fmt.Errorf("failed to check directory: %w", err)
	}
	
	var dirID string
	var dirExists bool
	
	if rows.Next() {
		if err := rows.Scan(&dirID); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan directory ID: %w", err)
		}
		dirExists = true
	}
	rows.Close()
	
	if !dirExists {
		return fmt.Errorf("directory not found: %s", path)
	}
	
	// Now list the contents of the directory
	if s.state.PointInTime != nil {
		query = `
			SELECT id, type, name, metadata
			FROM resources
			WHERE parent_id = ? AND valid_from <= ? 
			AND (valid_to IS NULL OR valid_to > ?)
			ORDER BY type DESC, name ASC
		`
	} else {
		query = `
			SELECT id, type, name, metadata
			FROM resources
			WHERE parent_id = ? AND valid_to IS NULL
			ORDER BY type DESC, name ASC
		`
	}
	
	// Execute query to get the directory contents
	if s.state.PointInTime != nil {
		pointInTime := *s.state.PointInTime
		if s.state.CurrentTransaction != nil {
			rows, err = s.state.CurrentTransaction.ExecuteQuery(query, dirID, pointInTime, pointInTime)
		} else {
			rows, err = s.db.ExecuteQuery(query, dirID, pointInTime, pointInTime)
		}
	} else {
		if s.state.CurrentTransaction != nil {
			rows, err = s.state.CurrentTransaction.ExecuteQuery(query, dirID)
		} else {
			rows, err = s.db.ExecuteQuery(query, dirID)
		}
	}
	
	if err != nil {
		return fmt.Errorf("failed to list directory: %w", err)
	}
	defer rows.Close()
	
	// Display the directory contents
	fmt.Printf("Contents of %s:\n", path)
	var hasContents bool
	
	for rows.Next() {
		hasContents = true
		var id, resType, name string
		var metadataStr string
		
		if err := rows.Scan(&id, &resType, &name, &metadataStr); err != nil {
			return fmt.Errorf("failed to scan resource: %w", err)
		}
		
		// Display based on type
		if resType == "directory" {
			fmt.Printf("%s/\n", name)
		} else if resType == "file" {
			// Try to parse metadata for size
			var metadata schema.ResourceMetadata
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err == nil {
				fmt.Printf("%s (%s)\n", name, formatSize(metadata.Size))
			} else {
				fmt.Printf("%s\n", name)
			}
		} else if resType == "symlink" {
			// Try to parse metadata for target
			var metadata schema.ResourceMetadata
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err == nil {
				fmt.Printf("%s -> %s\n", name, metadata.SymlinkTarget)
			} else {
				fmt.Printf("%s (symlink)\n", name)
			}
		} else {
			fmt.Printf("%s (%s)\n", name, resType)
		}
	}
	
	if !hasContents {
		fmt.Println("(empty directory)")
	}
	
	return nil
}

// BeginTransaction starts a new transaction
func (s *Shell) BeginTransaction() error {
	if s.state.CurrentTransaction != nil {
		return fmt.Errorf("transaction already in progress")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	tx.SetBranchID(s.state.CurrentBranch)
	tx.SetUserID(s.state.User)

	s.state.CurrentTransaction = tx

	fmt.Printf("Transaction T%s started\n", tx.GetID()[:8])
	return nil
}

// CommitTransaction commits the current transaction
func (s *Shell) CommitTransaction() error {
	if s.state.CurrentTransaction == nil {
		return fmt.Errorf("no transaction in progress")
	}

	err := s.state.CurrentTransaction.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Transaction T%s committed\n", s.state.CurrentTransaction.GetID()[:8])
	s.state.CurrentTransaction = nil
	return nil
}

// AbortTransaction aborts the current transaction
func (s *Shell) AbortTransaction() error {
	if s.state.CurrentTransaction == nil {
		return fmt.Errorf("no transaction in progress")
	}

	err := s.state.CurrentTransaction.Rollback()
	if err != nil {
		return fmt.Errorf("failed to abort transaction: %w", err)
	}

	fmt.Printf("Transaction T%s aborted\n", s.state.CurrentTransaction.GetID()[:8])
	s.state.CurrentTransaction = nil
	return nil
}

// ShowHistory shows command history
func (s *Shell) ShowHistory(args []string) error {
	// If args provided, show resource history
	if len(args) > 0 {
		return s.ShowResourceHistory(args[0])
	}

	// Otherwise show command history
	for i, cmd := range s.history {
		fmt.Printf("%d: %s\n", i+1, cmd)
	}
	return nil
}

// ShowResourceHistory shows the history of a resource
func (s *Shell) ShowResourceHistory(path string) error {
	// Implementation omitted for brevity
	// In a real implementation, this would query the database for resource history
	fmt.Printf("History for %s would appear here\n", path)
	return nil
}

// ExecuteQuery executes a SQL query
func (s *Shell) ExecuteQuery(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("query required")
	}

	query := strings.Join(args, " ")

	var result *database.QueryResult
	var err error

	options := database.DefaultQueryOptions()
	options.BranchID = s.state.CurrentBranch
	options.PointInTime = s.state.PointInTime

	if s.state.CurrentTransaction != nil {
		result, err = s.state.CurrentTransaction.Query(query, options)
	} else {
		result, err = s.db.Query(query, options)
	}

	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Format and display results
	if result.Count == 0 {
		fmt.Println("No results")
		return nil
	}

	// Print column headers
	for i, col := range result.Columns {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(col)
	}
	fmt.Println()

	// Print separator
	for i := 0; i < len(result.Columns); i++ {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print("--------")
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		for i, val := range row {
			if i > 0 {
				fmt.Print("\t")
			}
			if val == nil {
				fmt.Print("NULL")
			} else {
				fmt.Print(val)
			}
		}
		fmt.Println()
	}

	fmt.Printf("%d row(s) returned\n", result.Count)
	return nil
}

// SetPointInTime sets the point in time for time travel
func (s *Shell) SetPointInTime(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("time specification required")
	}

	timeSpec := args[0]
	var t time.Time
	var err error

	// Handle special time formats
	switch timeSpec {
	case "now":
		return s.ResetPointInTime()
	case "yesterday":
		t = time.Now().AddDate(0, 0, -1)
	case "last-week":
		t = time.Now().AddDate(0, 0, -7)
	case "last-month":
		t = time.Now().AddDate(0, -1, 0)
	default:
		// Try to parse as RFC3339
		t, err = time.Parse(time.RFC3339, timeSpec)
		if err != nil {
			// Try simpler formats
			t, err = time.Parse("2006-01-02", timeSpec)
			if err != nil {
				t, err = time.Parse("2006-01-02 15:04:05", timeSpec)
				if err != nil {
					return fmt.Errorf("invalid time format: %s", timeSpec)
				}
			}
		}
	}

	s.state.PointInTime = &t
	fmt.Printf("Time travel mode: viewing system state as of %s\n", t.Format(time.RFC3339))
	return nil
}

// ResetPointInTime returns to present time
func (s *Shell) ResetPointInTime() error {
	s.state.PointInTime = nil
	fmt.Println("Returned to present time")
	return nil
}

// ManageBranch manages branches
func (s *Shell) ManageBranch(args []string) error {
	// Implementation omitted for brevity
	fmt.Println("Branch management would appear here")
	return nil
}

// SwitchBranch switches to a different branch
func (s *Shell) SwitchBranch(args []string) error {
	// Implementation omitted for brevity
	fmt.Println("Branch switching would appear here")
	return nil
}

// MakeDirectory creates a new directory
func (s *Shell) MakeDirectory(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("directory name required")
	}
	
	dirName := args[0]
	var path string
	
	// Handle absolute vs relative paths
	if strings.HasPrefix(dirName, "/") {
		path = dirName
	} else {
		path = filepath.Join(s.state.CurrentDirectory, dirName)
	}
	
	// Normalize path
	path = filepath.Clean(path)
	
	// Extract the parent directory path and the new directory name
	parentPath := filepath.Dir(path)
	newDirName := filepath.Base(path)
	
	// Verify parent directory exists
	query := `
		SELECT id FROM resources 
		WHERE type = 'directory' AND path = ? AND valid_to IS NULL
	`
	
	rows, err := s.db.ExecuteQuery(query, parentPath)
	if err != nil {
		return fmt.Errorf("failed to check parent directory: %w", err)
	}
	
	var parentID string
	var parentExists bool
	
	if rows.Next() {
		if err := rows.Scan(&parentID); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan parent directory ID: %w", err)
		}
		parentExists = true
	}
	rows.Close()
	
	if !parentExists {
		return fmt.Errorf("parent directory not found: %s", parentPath)
	}
	
	// Check if directory already exists
	query = `
		SELECT 1 FROM resources 
		WHERE parent_id = ? AND name = ? AND type = 'directory' AND valid_to IS NULL
	`
	
	existsRows, err := s.db.ExecuteQuery(query, parentID, newDirName)
	if err != nil {
		return fmt.Errorf("failed to check if directory exists: %w", err)
	}
	
	var exists bool
	if existsRows.Next() {
		exists = true
	}
	existsRows.Close()
	
	if exists {
		return fmt.Errorf("directory already exists: %s", path)
	}
	
	// Start a transaction if one isn't already active
	var tx *database.Transaction
	var newTx bool
	
	if s.state.CurrentTransaction != nil {
		tx = s.state.CurrentTransaction
	} else {
		var err error
		tx, err = s.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		newTx = true
		defer func() {
			if newTx && tx.IsActive() {
				tx.Rollback()
			}
		}()
	}
	
	// Create the directory
	dirID := fmt.Sprintf("dir-%d", time.Now().UnixNano())
	
	// Create directory metadata
	metadata := schema.NewDirectoryMetadata(s.state.User)
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal directory metadata: %w", err)
	}
	
	// Insert the directory
	now := time.Now()
	_, err = tx.Execute(`
		INSERT INTO resources (id, type, name, parent_id, path, metadata, valid_from, transaction_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, dirID, schema.ResourceTypeDirectory, newDirName, parentID, path, string(metadataJSON), now, tx.GetID())
	
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	
	// If we started a new transaction, commit it
	if newTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	
	fmt.Printf("Directory created: %s\n", path)
	return nil
}

// TouchFile creates an empty file
func (s *Shell) TouchFile(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("file name required")
	}
	
	fileName := args[0]
	var path string
	
	// Handle absolute vs relative paths
	if strings.HasPrefix(fileName, "/") {
		path = fileName
	} else {
		path = filepath.Join(s.state.CurrentDirectory, fileName)
	}
	
	// Normalize path
	path = filepath.Clean(path)
	
	// Extract the parent directory path and the new file name
	parentPath := filepath.Dir(path)
	newFileName := filepath.Base(path)
	
	// Verify parent directory exists
	query := `
		SELECT id FROM resources 
		WHERE type = 'directory' AND path = ? AND valid_to IS NULL
	`
	
	rows, err := s.db.ExecuteQuery(query, parentPath)
	if err != nil {
		return fmt.Errorf("failed to check parent directory: %w", err)
	}
	
	var parentID string
	var parentExists bool
	
	if rows.Next() {
		if err := rows.Scan(&parentID); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan parent directory ID: %w", err)
		}
		parentExists = true
	}
	rows.Close()
	
	if !parentExists {
		return fmt.Errorf("parent directory not found: %s", parentPath)
	}
	
	// Check if file already exists
	query = `
		SELECT 1 FROM resources 
		WHERE parent_id = ? AND name = ? AND valid_to IS NULL
	`
	
	existsRows, err := s.db.ExecuteQuery(query, parentID, newFileName)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}
	
	var exists bool
	if existsRows.Next() {
		exists = true
	}
	existsRows.Close()
	
	if exists {
		// File exists, update its timestamp
		query = `
			UPDATE resources 
			SET valid_to = ?
			WHERE parent_id = ? AND name = ? AND valid_to IS NULL
		`
		
		now := time.Now()
		
		// Start a transaction if one isn't already active
		var tx *database.Transaction
		var newTx bool
		
		if s.state.CurrentTransaction != nil {
			tx = s.state.CurrentTransaction
		} else {
			tx, err = s.db.Begin()
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			newTx = true
			defer func() {
				if newTx && tx.IsActive() {
					tx.Rollback()
				}
			}()
		}
		
		// Mark the old version as obsolete
		_, err = tx.Execute(query, now, parentID, newFileName)
		if err != nil {
			return fmt.Errorf("failed to update file: %w", err)
		}
		
		// Get the old file's details
		query = `
			SELECT id, content, metadata
			FROM resources 
			WHERE parent_id = ? AND name = ? AND valid_to = ?
		`
		
		detailRows, err := tx.ExecuteQuery(query, parentID, newFileName, now)
		if err != nil {
			return fmt.Errorf("failed to get file details: %w", err)
		}
		
		var oldID string
		var content []byte
		var metadataStr string
		
		if detailRows.Next() {
			if err := detailRows.Scan(&oldID, &content, &metadataStr); err != nil {
				detailRows.Close()
				return fmt.Errorf("failed to scan file details: %w", err)
			}
		}
		detailRows.Close()
		
		// Parse metadata to update timestamp
		var metadata schema.ResourceMetadata
		if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
		
		metadata.ModifiedAt = now
		metadata.AccessedAt = now
		
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
		
		// Create a new version of the file
		fileID := fmt.Sprintf("file-%d", time.Now().UnixNano())
		
		_, err = tx.Execute(`
			INSERT INTO resources (id, type, name, parent_id, path, content, metadata, valid_from, transaction_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, fileID, schema.ResourceTypeFile, newFileName, parentID, path, content, string(metadataJSON), now, tx.GetID())
		
		if err != nil {
			return fmt.Errorf("failed to create new file version: %w", err)
		}
		
		// If we started a new transaction, commit it
		if newTx {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
		}
		
		fmt.Printf("File updated: %s\n", path)
	} else {
		// File doesn't exist, create it
		fileID := fmt.Sprintf("file-%d", time.Now().UnixNano())
		
		// Create file metadata
		metadata := schema.NewResourceMetadata(s.state.User)
		metadata.Size = 0 // Empty file
		
		// Determine MIME type based on extension
		ext := strings.ToLower(filepath.Ext(newFileName))
		switch ext {
		case ".txt":
			metadata.MimeType = "text/plain"
		case ".html", ".htm":
			metadata.MimeType = "text/html"
		case ".json":
			metadata.MimeType = "application/json"
		case ".md":
			metadata.MimeType = "text/markdown"
		case ".go":
			metadata.MimeType = "text/x-go"
		default:
			metadata.MimeType = "application/octet-stream"
		}
		
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
		
		// Start a transaction if one isn't already active
		var tx *database.Transaction
		var newTx bool
		
		if s.state.CurrentTransaction != nil {
			tx = s.state.CurrentTransaction
		} else {
			tx, err = s.db.Begin()
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			newTx = true
			defer func() {
				if newTx && tx.IsActive() {
					tx.Rollback()
				}
			}()
		}
		
		// Insert the file
		now := time.Now()
		_, err = tx.Execute(`
			INSERT INTO resources (id, type, name, parent_id, path, content, metadata, valid_from, transaction_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, fileID, schema.ResourceTypeFile, newFileName, parentID, path, []byte{}, string(metadataJSON), now, tx.GetID())
		
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		
		// If we started a new transaction, commit it
		if newTx {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
		}
		
		fmt.Printf("File created: %s\n", path)
	}
	
	return nil
}

// RemoveResource removes a resource
func (s *Shell) RemoveResource(args []string) error {
	// Implementation omitted for brevity
	fmt.Println("Resource removal would appear here")
	return nil
}

// CatFile displays file contents
func (s *Shell) CatFile(args []string) error {
	// Implementation omitted for brevity
	fmt.Println("File contents would appear here")
	return nil
}

// Echo writes text to a file
func (s *Shell) Echo(args []string) error {
	// Implementation omitted for brevity
	fmt.Println("Echo command would appear here")
	return nil
}