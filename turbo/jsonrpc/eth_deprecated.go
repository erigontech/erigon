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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
)

// Accounts implements eth_accounts. Returns a list of addresses owned by the client.
// Deprecated: This function will be removed in the future.
func (api *APIImpl) Accounts(ctx context.Context) ([]common.Address, error) {
	return []common.Address{}, fmt.Errorf(NotAvailableDeprecated, "eth_accounts")
}

// Sign implements eth_sign. Calculates an Ethereum specific signature with: sign(keccak256('\\x19Ethereum Signed Message:\\n' + len(message) + message))).
// Deprecated: This function will be removed in the future.
func (api *APIImpl) Sign(ctx context.Context, _ common.Address, _ hexutility.Bytes) (hexutility.Bytes, error) {
	return hexutility.Bytes(""), fmt.Errorf(NotAvailableDeprecated, "eth_sign")
}

// SignTransaction deprecated
func (api *APIImpl) SignTransaction(_ context.Context, txObject interface{}) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotAvailableDeprecated, "eth_signTransaction")
}
