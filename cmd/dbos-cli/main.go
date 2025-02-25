package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/yourusername/dbos/pkg/database"
	"github.com/yourusername/dbos/pkg/schema"
	"github.com/yourusername/dbos/pkg/shell"
	"github.com/yourusername/dbos/internal/util"
)

var (
	// Command line flags
	dbType      = flag.String("db", "sqlite", "Database type (sqlite, postgres, inmemory)")
	dbPath      = flag.String("path", "", "Database path or connection string")
	interactive = flag.Bool("i", true, "Run in interactive mode")
	version     = flag.Bool("version", false, "Show version information")
)

const (
	AppVersion = "0.1.0"
)

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("DBOS CLI v%s\n", AppVersion)
		return
	}

	// Set default database path if not specified
	if *dbPath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting home directory: %v\n", err)
			os.Exit(1)
		}
		*dbPath = filepath.Join(homeDir, ".dbos", "dbos.db")
	}

	// Ensure data directory exists
	dataDir := filepath.Dir(*dbPath)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating data directory: %v\n", err)
		os.Exit(1)
	}

	// Initialize database connection
	db, err := database.Connect(*dbType, *dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Initialize database schema
	if err := schema.Initialize(db); err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing schema: %v\n", err)
		os.Exit(1)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down DBOS...")
		db.Close()
		os.Exit(0)
	}()

	// Execute commands from arguments if not in interactive mode
	if !*interactive && len(flag.Args()) > 0 {
		cmd := flag.Args()[0]
		args := flag.Args()[1:]
		if err := executeCommand(db, cmd, args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Start interactive shell
	if *interactive {
		fmt.Printf("DBOS CLI v%s - Database Operating System\n", AppVersion)
		fmt.Println("Type 'help' for available commands")
		shell := shell.NewShell(db)
		shell.Run()
	}
}

func executeCommand(db *database.Connection, cmd string, args []string) error {
	// TODO: Implement command execution logic
	return fmt.Errorf("Command execution not implemented yet")
}