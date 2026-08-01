// Copyright 2026 The Erigon Authors
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

package jsonrpc

import "github.com/erigontech/erigon/common/hexutil"

// WitnessParity is the outcome of comparing an erigon-built witness against a
// reference (reth) witness. A trie node's RLP bytes are its identity, so set
// membership is a raw byte comparison — no keccak needed. State nodes, codes,
// and keys are each treated as sets: duplicates collapse and order is irrelevant.
type WitnessParity struct {
	// ByteIdentical is true when the two state-node sets contain exactly the same
	// node bytes (ErigonOnly == 0 && RethOnly == 0).
	ByteIdentical bool
	// ErigonOnly counts state nodes present in the erigon set but absent from reth.
	// erigon's minimal witness is expected to be a strict subset of reth's, so a
	// non-zero value is the red flag (a node reth would not have included).
	ErigonOnly int
	// RethOnly counts state nodes present in the reth set but absent from erigon —
	// benign after-state / read-touched nodes erigon's recompute witness omits.
	RethOnly int
	// CodesEqual is true when both witnesses carry the same set of bytecode blobs.
	CodesEqual bool
	// KeysEqual is true when both witnesses carry the same set of preimage keys.
	KeysEqual bool
}

// CompareWitnessNodeSets compares an erigon-built witness against a reference reth
// witness by raw node bytes. It is pure: no I/O, no datadir, no keccak. A nil result
// is treated as empty sets.
func CompareWitnessNodeSets(erigon, reth *ExecutionWitnessResult) WitnessParity {
	var eState, eCodes, eKeys, rState, rCodes, rKeys []hexutil.Bytes
	if erigon != nil {
		eState, eCodes, eKeys = erigon.State, erigon.Codes, erigon.Keys
	}
	if reth != nil {
		rState, rCodes, rKeys = reth.State, reth.Codes, reth.Keys
	}

	erigonState, rethState := byteSet(eState), byteSet(rState)
	erigonOnly := countMissing(erigonState, rethState)
	rethOnly := countMissing(rethState, erigonState)

	return WitnessParity{
		ByteIdentical: erigonOnly == 0 && rethOnly == 0,
		ErigonOnly:    erigonOnly,
		RethOnly:      rethOnly,
		CodesEqual:    setsEqual(byteSet(eCodes), byteSet(rCodes)),
		KeysEqual:     setsEqual(byteSet(eKeys), byteSet(rKeys)),
	}
}

func byteSet(items []hexutil.Bytes) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, it := range items {
		set[string(it)] = struct{}{}
	}
	return set
}

// countMissing returns the number of keys present in a but absent from b.
func countMissing(a, b map[string]struct{}) int {
	n := 0
	for k := range a {
		if _, ok := b[k]; !ok {
			n++
		}
	}
	return n
}

func setsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	return countMissing(a, b) == 0
}
