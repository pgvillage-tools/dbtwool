// Package utils holds all utils for handling files and other global operations
package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveHome can resolve the home folder of the current user
func ResolveHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		return filepath.Join(home, path[2:])
	}
	return filepath.Clean(path)
}

// MakeTree can be used to create a folder with all parent folders
func MakeTree(folderPath string) error {
	return os.MkdirAll(ResolveHome(folderPath), os.ModePerm)
}

// AbsolutePath can be used to create an absolute path from a relative path
func AbsolutePath(relativePath string) (absPath string, err error) {
	absPath = ResolveHome(relativePath)
	if absPath, err = filepath.EvalSymlinks(absPath); err != nil {
		return "", fmt.Errorf("failed to resolve symlink %s: %w", absPath, err)
	} else if absPath, err = filepath.Abs(absPath); err != nil {
		return "", fmt.Errorf("failed to get absolute path for %s: %w", absPath, err)
	}
	return absPath, nil
}

// GetEnv can be used to retrieve a value from the environment returning a default when unset
func GetEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
