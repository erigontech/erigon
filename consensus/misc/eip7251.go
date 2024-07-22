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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

func DequeueConsolidationRequests7251(syscall consensus.SystemCall) types.Requests {
	res, err := syscall(params.ConsolidationRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to ConsolidationRequestAddress", "err", err)
		return nil
	}
	// Parse out the consolidations - using the bytes array returned
	var reqs types.Requests
	lenPerReq := 20 + 48 + 48 // addr + sourcePubkey + targetPubkey
	for i := 0; i <= len(res)-lenPerReq; i += lenPerReq {
		var sourcePubKey [48]byte
		copy(sourcePubKey[:], res[i+20:i+68])
		var targetPubKey [48]byte
		copy(targetPubKey[:], res[i+68:i+116])
		wr := &types.ConsolidationRequest{
			SourceAddress: common.BytesToAddress(res[i : i+20]),
			SourcePubKey:  sourcePubKey,
			TargetPubKey:  targetPubKey,
		}
		reqs = append(reqs, wr)
	}
	return reqs
}
