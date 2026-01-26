package db2client

// ConnParams is an interface for a struct to hold connection params and build a
// Connection String from it as needed
type ConnParams interface {
	GetConnString() string
}
