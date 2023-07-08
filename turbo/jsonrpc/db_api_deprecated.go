package jsonrpc

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

// DBAPI the interface for the db_ RPC commands (deprecated)
type DBAPI interface {
	GetString(_ context.Context, _ string, _ string) (string, error)
	PutString(_ context.Context, _ string, _ string, _ string) (bool, error)
	GetHex(_ context.Context, _ string, _ string) (hexutility.Bytes, error)
	PutHex(_ context.Context, _ string, _ string, _ hexutility.Bytes) (bool, error)
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

// GetString implements db_getString. Returns string from the local database.
// Deprecated: This function will be removed in the future.
func (api *DBAPIImpl) GetString(_ context.Context, _ string, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "db_getString")
}

// PutString implements db_putString. Stores a string in the local database.
// Deprecated: This function will be removed in the future.
func (api *DBAPIImpl) PutString(_ context.Context, _ string, _ string, _ string) (bool, error) {
	return false, fmt.Errorf(NotAvailableDeprecated, "db_putString")
}

// GetHex implements db_getHex. Returns binary data from the local database.
// Deprecated: This function will be removed in the future.
func (api *DBAPIImpl) GetHex(_ context.Context, _ string, _ string) (hexutility.Bytes, error) {
	return hexutility.Bytes(""), fmt.Errorf(NotAvailableDeprecated, "db_getHex")
}

// PutHex implements db_putHex. Stores binary data in the local database.
// Deprecated: This function will be removed in the future.
func (api *DBAPIImpl) PutHex(_ context.Context, _ string, _ string, _ hexutility.Bytes) (bool, error) {
	return false, fmt.Errorf(NotAvailableDeprecated, "db_putHex")
}
