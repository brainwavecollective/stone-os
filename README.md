# Stone-OS
Stonebraker Inspired Database Operating System (DBOS) Exploratory Project
 
On February 25th 2025 I was having a discussion with a friend regarding ideas around a database operating system and what it would take to create such a system. The below README content and related repo is the result of a few brief conversations with Claude 3.7 extended. You can see that conversation here:  
https://claude.ai/share/67f577c4-ead8-4809-a76b-7cecce7dbe70  

Stone OS is an innovative operating system built on the concept of using an ACID-compliant database as the underlying foundation to manage state changes over time, inspired by Michael Stonebraker's concepts.

Although I've thought this is a super interesting project from the perspective of versioning, and I am curious what it would take to get this across the line, I don't have any specific plans to pursue at the moment. 

This project was inspired by, but is unrelated to Michael Stonebraker. To see some of his related work check out: https://www.dbos.dev/  


## Overview

DBOS represents a fundamental shift in operating system design by:

- Using a database as the core foundation for all operations
- Enabling time-travel capabilities through temporal tables
- Providing transaction-based command processing
- Maintaining complete history and auditability
- Supporting branching and parallel states

## Key Features

- **Database-backed System**: All resources (files, directories, processes) are database entities
- **Time Travel**: Navigate to any previous system state
- **ACID Properties**: Transactional integrity for all operations
- **Branching**: Create isolated environments for experimentation
- **Audit Trail**: Complete history of all system changes
- **SQL Interface**: Query your system state using familiar SQL

## Getting Started

### Prerequsites 
 1. [Install Go](https://go.dev/doc/install)

### Installation

```bash
# Clone the repository
git clone https://github.com/brainwavecollective/stone-os.git
cd stone-os

# Build the CLI
make build

# Run the CLI
./bin/dbos-cli
```

### Basic Usage

```bash
# Start the DBOS shell
dbos-cli

# Create a file
dbos> touch newfile.txt
dbos> echo "Hello DBOS" > newfile.txt

# View system resources
dbos> ls

# Start a transaction
dbos> begin
Transaction T123 started

# Make changes within the transaction
dbos (T123)> rm newfile.txt
dbos (T123)> commit

# View history of a file
dbos> history newfile.txt

# Create a branch for experimentation
dbos> branch experimental
dbos> switch experimental
```

## Architecture

DBOS is built on these core components:

1. **Database Layer**: PostgreSQL (or SQLite for lightweight usage)
2. **Virtual Filesystem**: Database-backed file operations
3. **Transaction Manager**: Handles ACID properties for all operations
4. **Time Travel Engine**: Enables historical state navigation
5. **Branching System**: Manages parallel system states
6. **Command Interpreter**: Translates shell commands to database operations

## Use Cases

- **Development Environments**: Isolated branches for different features
- **System Administration**: Roll back to known good states after issues
- **Compliance & Auditing**: Complete traceability of all system changes
- **Education**: Understand system evolution over time
- **Debugging**: Reproduce issues by returning to prior states

## Contributing

Get at me if you have an interest in contributing to this project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This project is inspired by Michael Stonebraker's Database Operating System concept and builds upon decades of research in database systems and operating systems design.
