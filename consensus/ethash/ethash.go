// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package ethash implements the ethash proof-of-work consensus engine.
package ethash

import (
	"errors"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
)

var ErrInvalidDumpMagic = errors.New("invalid dump magic")

// New creates a full sized ethash PoW scheme and starts a background thread for
// remote mining, also optionally notifying a batch of remote services of new work
// packages.
func New(notify []string, noverify bool) BaseFake {
	return BaseFake{}
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c BaseFake) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{
		//{
		//Namespace: "clique",
		//Version:   "1.0",
		//Service:   &API{chain: chain, clique: c},
		//Public:    false,
		//}
	}
}

func (BaseFake) Close() error {
	return nil
}

func (BaseFake) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (BaseFake) GetTransferFunc() evmtypes.TransferFunc {
	return consensus.Transfer
}

func (BaseFake) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	return false
}
