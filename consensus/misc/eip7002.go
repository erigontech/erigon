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

func DequeueWithdrawalRequests7002(syscall consensus.SystemCall) types.Request {
	res, err := syscall(params.WithdrawalRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to WithdrawalRequestAddress", "err", err)
		return nil
	}
	// Just append the contract outputs
	return &types.FlatRequest{Type: types.WithdrawalRequestType, RequestData: res}
}
