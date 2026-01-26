package pg

import "strings"

// IsolationLevel is used to get rdbms specific queries for basic functions
type IsolationLevel int

const (
	// ReadCommitted defines the READ COMMITTED isolation level
	ReadCommitted IsolationLevel = iota
	// RepeatableRead defines the REPEATABLE READ isolation level
	RepeatableRead
	// Serializable defines the SERIALIZABLE isolation level
	Serializable
)

var (
	levelToSting = map[IsolationLevel]string{
		ReadCommitted:  "READ COMMITTED",
		RepeatableRead: "REPEATABLE READ",
		Serializable:   "SERIALIZABLE",
	}
)

// AsQuery can be used to return a query for the isolation level
func (i IsolationLevel) AsQuery() string {
	return strings.Join([]string{
		"SET",
		"TRANSACTION",
		"ISOLATION",
		"LEVEL",
		i.AsString(),
	}, " ")
}

// AsString can be used to return a string version of the isolation level
func (i IsolationLevel) AsString() string {
	return levelToSting[i]
}

// GetIsolationLevel can be used to get the correct Isolation level from an index
func GetIsolationLevel(i int) IsolationLevel {
	return ReadCommitted + IsolationLevel(i)
}
