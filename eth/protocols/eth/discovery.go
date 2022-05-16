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
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/erigon/rlp"
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

// CurrentENREntryFromForks constructs an `eth` ENR entry based on the current state of the chain.
func CurrentENREntryFromForks(forks []uint64, genesisHash common.Hash, headHeight uint64) *enrEntry {
	return &enrEntry{
		ForkID: forkid.NewIDFromForks(forks, genesisHash, headHeight),
	}
}

func LoadENRForkID(r *enr.Record) (*forkid.ID, error) {
	var entry enrEntry
	if err := r.Load(&entry); err != nil {
		if enr.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load fork ID from ENR: %w", err)
	}
	return &entry.ForkID, nil
}
