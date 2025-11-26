package pgclient

// IsolationLevel is a enum for cusror isolation levels
type IsolationLevel string

const (
	// defines a cursor isolation level for reading committed data
	IsoLevelReadCommitted = "READ COMMITTED"
	// IsoLevelCursorSerializable defines a cursor isolation level for serializable reading
	IsoLevelCursorSerializable = "SERIALIZABLE"
)
