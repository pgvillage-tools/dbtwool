package db2client

// IsolationLevel is a enum for cusror isolation levels
type IsolationLevel string

const (
	// IsoLevelReadUncommitted defines a cursor isolation level for reading uncommitted data
	IsoLevelUncommittedRead = "UC"
	// IsoLevelCusrorStability defines a cursor isolation level for reading committed data from a snapshot of this cursor
	IsoLevelCursorStability = "CS"
)
