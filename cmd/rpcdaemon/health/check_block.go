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

package health

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/rpc"
)

func checkBlockNumber(blockNumber rpc.BlockNumber, api EthAPI) error {
	if api == nil {
		return errors.New("no connection to the Erigon server or `eth` namespace isn't enabled")
	}
	data, err := api.GetBlockByNumber(context.TODO(), blockNumber, false)
	if err != nil {
		return err
	}
	if len(data) == 0 { // block not found
		return fmt.Errorf("no known block with number %v (%x hex)", blockNumber.Uint64(), blockNumber.Uint64())
	}

	return nil
}
