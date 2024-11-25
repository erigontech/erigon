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

package ethapi

// This file stores proxy-objects for `internal` package
import (
	libcommon "github.com/erigontech/erigon/erigon-lib/common"

	"github.com/erigontech/erigon/core/types"
)

// nolint
func RPCMarshalBlock(b *types.Block, inclTx bool, fullTx bool, additional map[string]interface{}) (map[string]interface{}, error) {
	fields, err := RPCMarshalBlockDeprecated(b, inclTx, fullTx)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}

// nolint
func RPCMarshalBlockEx(b *types.Block, inclTx bool, fullTx bool, borTx types.Transaction, borTxHash libcommon.Hash, additional map[string]interface{}) (map[string]interface{}, error) {
	fields, err := RPCMarshalBlockExDeprecated(b, inclTx, fullTx, borTx, borTxHash)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}
