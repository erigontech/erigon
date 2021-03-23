// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// enrEntry is the ENR entry which advertises `eth` protocol on the discovery.
type enrEntry struct {
	ForkID forkid.ID // Fork identifier per EIP-2124

	// Ignore additional fields (for forward compatibility).
	Rest []rlp.RawValue `rlp:"tail"`
}

// ENRKey implements enr.Entry.
func (e enrEntry) ENRKey() string {
	return "eth"
}

// StartENRUpdater starts the `eth` ENR updater loop, which listens for chain
// head events and updates the requested node record whenever a fork is passed.
func StartENRUpdater(chainConfig *params.ChainConfig, genesisHash common.Hash, events *remotedbserver.Events, ln *enode.LocalNode) {
	events.AddHeaderSubscription(func(h *types.Header) error {
		ln.Set(currentENREntry(chainConfig, genesisHash, h.Number.Uint64()))
		return nil
	})
}

// currentENREntry constructs an `eth` ENR entry based on the current state of the chain.
func currentENREntry(chainConfig *params.ChainConfig, genesisHash common.Hash, headHeight uint64) *enrEntry {
	return &enrEntry{
		ForkID: forkid.NewID(chainConfig, genesisHash, headHeight),
	}
}
