package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CreateDirectory creates a directory if it doesn't exist
func CreateDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

// FormatByteSize formats a byte size into a human-readable string
func FormatByteSize(bytes int64) string {
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

// FormatTimestamp formats a timestamp
func FormatTimestamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

// CalculateChecksum calculates a SHA-256 checksum of data
func CalculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// SplitPath splits a path into its components
func SplitPath(path string) []string {
	// Normalize path
	path = filepath.Clean(path)
	
	// Split path
	components := strings.Split(path, string(os.PathSeparator))
	
	// Filter out empty components
	var result []string
	for _, component := range components {
		if component != "" {
			result = append(result, component)
		}
	}
	
	return result
}

// JoinPath joins path components
func JoinPath(components ...string) string {
	return filepath.Join(components...)
}

// IsAbsolutePath checks if a path is absolute
func IsAbsolutePath(path string) bool {
	return filepath.IsAbs(path)
}

// GetRelativePath gets a path relative to a base path
func GetRelativePath(basePath, path string) (string, error) {
	return filepath.Rel(basePath, path)
}

// ParseTimeSpec parses a time specification string
func ParseTimeSpec(timeSpec string) (time.Time, error) {
	// Handle special time formats
	switch timeSpec {
	case "now":
		return time.Now(), nil
	case "yesterday":
		return time.Now().AddDate(0, 0, -1), nil
	case "last-week":
		return time.Now().AddDate(0, 0, -7), nil
	case "last-month":
		return time.Now().AddDate(0, -1, 0), nil
	}
	
	// Try to parse as RFC3339
	if t, err := time.Parse(time.RFC3339, timeSpec); err == nil {
		return t, nil
	}
	
	// Try simpler formats
	if t, err := time.Parse("2006-01-02", timeSpec); err == nil {
		return t, nil
	}
	
	if t, err := time.Parse("2006-01-02 15:04:05", timeSpec); err == nil {
		return t, nil
	}
	
	return time.Time{}, fmt.Errorf("invalid time format: %s", timeSpec)
}

// ValidateResourceName validates a resource name
func ValidateResourceName(name string) error {
	if name == "" {
		return fmt.Errorf("resource name cannot be empty")
	}
	
	if strings.Contains(name, "/") {
		return fmt.Errorf("resource name cannot contain '/'")
	}
	
	if name == "." || name == ".." {
		return fmt.Errorf("resource name cannot be '.' or '..'")
	}
	
	return nil
}

// GetHomeDirectory gets the user's home directory
func GetHomeDirectory() (string, error) {
	return os.UserHomeDir()
}

// GetAppDataDirectory gets the application data directory
func GetAppDataDirectory(appName string) (string, error) {
	home, err := GetHomeDirectory()
	if err != nil {
		return "", err
	}
	
	appDir := filepath.Join(home, "."+appName)
	if err := CreateDirectory(appDir); err != nil {
		return "", err
	}
	
	return appDir, nil
}