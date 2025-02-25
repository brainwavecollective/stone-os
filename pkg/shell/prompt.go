package shell

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yourusername/dbos/pkg/database"
)

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

		var input string
		if _, err := fmt.Scanln(&input); err != nil {
			if err.Error() == "unexpected newline" {
				// Empty input, ignore
				continue
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
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

	// Normalize path
	path = filepath.Clean(path)

	// Verify directory exists
	options := database.DefaultQueryOptions()
	options.BranchID = s.state.CurrentBranch
	options.PointInTime = s.state.PointInTime

	result, err := s.db.FindResourceByPath(path, options)
	if err != nil {
		return fmt.Errorf("failed to find directory: %w", err)
	}

	if result.Count == 0 {
		return fmt.Errorf("directory not found: %s", path)
	}

	// Check that it's a directory
	resourceType := result.Rows[0][1].(string)
	if resourceType != "directory" {
		return fmt.Errorf("not a directory: %s", path)
	}

	// Update current directory
	s.state.CurrentDirectory = path
	return nil
}

// ListDirectory lists the contents of a directory
func (s *Shell) ListDirectory(args []string) error {
	// Implementation omitted for brevity
	// In a real implementation, this would query the database for directory contents
	fmt.Println("Directory listing would appear here")
	return nil
}

// other command implementations would be here...

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

// Other command implementations would go here...

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
	// Implementation omitted for brevity
	fmt.Println("Directory creation would appear here")
	return nil
}

// TouchFile creates an empty file
func (s *Shell) TouchFile(args []string) error {
	// Implementation omitted for brevity
	fmt.Println("File creation would appear here")
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