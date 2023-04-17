package jsonrpc

import (
	"github.com/gorilla/websocket"
)

// storageInterface json rpc internal storage to persist data
type storageInterface interface {
	GetAllBlockFiltersWithWSConn() ([]*Filter, error)
	GetAllLogFiltersWithWSConn() ([]*Filter, error)
	GetFilter(filterID string) (*Filter, error)
	NewBlockFilter(wsConn *websocket.Conn) (string, error)
	NewLogFilter(wsConn *websocket.Conn, filter LogFilter) (string, error)
	NewPendingTransactionFilter(wsConn *websocket.Conn) (string, error)
	UninstallFilter(filterID string) error
	UninstallFilterByWSConn(wsConn *websocket.Conn) error
	UpdateFilterLastPoll(filterID string) error
}
