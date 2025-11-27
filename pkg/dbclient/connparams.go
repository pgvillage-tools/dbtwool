package dbclient

type ConnParams interface {
	GetConnString() string
}
