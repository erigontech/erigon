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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
)

// nolint
func RPCMarshalBlock(b *types.Block, inclTx bool, fullTx bool, additional map[string]interface{}, isArbitrumNitro bool) (map[string]interface{}, error) {
	fields, err := RPCMarshalBlockDeprecated(b, inclTx, fullTx, isArbitrumNitro)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}

// nolint
func RPCMarshalBlockEx(b *types.Block, inclTx bool, fullTx bool, borTx types.Transaction, borTxHash common.Hash, additional map[string]interface{}, isArbitrumNitro bool) (map[string]interface{}, error) {
	fields, err := RPCMarshalBlockExDeprecated(b, inclTx, fullTx, borTx, borTxHash, isArbitrumNitro)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}
