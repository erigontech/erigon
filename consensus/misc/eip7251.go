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

package misc

import (
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

const ConsolidationRequestDataLen = 116

func DequeueConsolidationRequests7251(syscall consensus.SystemCall) types.Requests {
	res, err := syscall(params.ConsolidationRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to ConsolidationRequestAddress", "err", err)
		return nil
	}
	// Just append the contract outputs as the encoded request data
	var reqs types.Requests
	for i := 0; i <= len(res)-ConsolidationRequestDataLen; i += ConsolidationRequestDataLen {
		wr := &types.ConsolidationRequest{
			RequestData: [ConsolidationRequestDataLen]byte(res[i : i+ConsolidationRequestDataLen]),
		}
		reqs = append(reqs, wr)
	}
	return reqs
}
