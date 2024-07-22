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
	"errors"
	"fmt"
	"net/http"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/rpc"
)

var (
	errTimestampTooOld = errors.New("timestamp too old")
)

func checkTime(
	r *http.Request,
	seconds int,
	ethAPI EthAPI,
) error {
	i, err := ethAPI.GetBlockByNumber(r.Context(), rpc.LatestBlockNumber, false)
	if err != nil {
		return err
	}
	timestamp := 0
	if ts, ok := i["timestamp"]; ok {
		if cs, ok := ts.(hexutil.Uint64); ok {
			timestamp = int(uint64(cs))
		} else if cs, ok := ts.(uint64); ok {
			timestamp = int(cs)
		}
	}
	if timestamp < seconds {
		return fmt.Errorf("%w: got ts: %d, need: %d", errTimestampTooOld, timestamp, seconds)
	}

	return nil
}
