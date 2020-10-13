package commands

import (
	"context"
	"fmt"
)

// DBAPI the interface for the db_ RPC commands (deprecated)
type DBAPI interface {
	GetString(_ context.Context, _ string, _ string) (string, error)
	PutString(_ context.Context, _ string, _ string, _ string) (string, error)
	GetHex(_ context.Context, _ string, _ string) (string, error)
	PutHex(_ context.Context, _ string, _ string, _ string) (string, error)
}

// DBAPIImpl data structure to store things needed for db_ commands
type DBAPIImpl struct {
	unused uint64
}

// NewDBAPIImpl returns NetAPIImplImpl instance
func NewDBAPIImpl() *DBAPIImpl {
	return &DBAPIImpl{
		unused: uint64(0),
	}
}

// GetString Returns string from the local database. (deprecated)
func (api *DBAPIImpl) GetString(_ context.Context, _ string, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "db_getString")
}

// PutString Stores a string in the local database. (deprecated)
func (api *DBAPIImpl) PutString(_ context.Context, _ string, _ string, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "db_putString")
}

// GetHex Returns binary data from the local database. (deprecated)
func (api *DBAPIImpl) GetHex(_ context.Context, _ string, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "db_getHex")
}

// PutHex Stores binary data in the local database. (deprecated)
func (api *DBAPIImpl) PutHex(_ context.Context, _ string, _ string, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "db_putHex")
}
