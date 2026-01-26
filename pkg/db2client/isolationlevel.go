package db2client

import "strings"

// IsolationLevel is used to get rdbms specific queries for basic functions
type IsolationLevel int

const (
	// UncommittedRead defines the Uncommitted Read isolation level
	UncommittedRead IsolationLevel = iota
	// ReadStability defines the Read Stability isolation level
	ReadStability
	// CursorStability defines the Cursor Stability isolation level
	CursorStability
	// RepeatableRead defines the Repeatable Read isolation level
	RepeatableRead
)

var (
	levelToSting = map[IsolationLevel]string{
		UncommittedRead: "UR",
		ReadStability:   "RS",
		CursorStability: "CS",
		RepeatableRead:  "RR",
	}
)

// AsQuery can be used to return a query for the isolation level
func (i IsolationLevel) AsQuery() string {
	return strings.Join([]string{
		"SET",
		"CURRENT",
		"ISOLATION",
		i.AsString(),
	}, " ")
}

// AsString can be used to return a string version of the isolation level
func (i IsolationLevel) AsString() string {
	return levelToSting[i]
}

// GetIsolationLevel can be used to get the correct Isolation level from an index
func GetIsolationLevel(i int) IsolationLevel {
	return UncommittedRead + IsolationLevel(i)
}
