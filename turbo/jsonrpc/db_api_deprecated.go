// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common/hexutility"
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
