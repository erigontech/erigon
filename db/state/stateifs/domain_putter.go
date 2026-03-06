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

package stateifs

import (
	"github.com/erigontech/erigon/db/kv"
)

// DomainPutter is an interface for putting data into domains.
// Used by commitment to write branch data.
// SharedDomains implements this interface directly.
type DomainPutter interface {
	DomainPut(domain kv.Domain, tx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error
}

// CommitmentWrite represents a commitment domain write that needs to be added to changesets.
type CommitmentWrite struct {
	Key     []byte
	Value   []byte
	TxNum   uint64
	PrevVal []byte
}
