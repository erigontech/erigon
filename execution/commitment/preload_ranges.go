// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import "github.com/erigontech/erigon/execution/commitment/nibbles"

// NextSubtree: exclusive upper bound of a prefix-range scan over `in`. Returns
// nil if `in` is all 0xff. Mirrors db/kv.NextSubtree (inlined to keep imports minimal).
func NextSubtree(in []byte) []byte {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 0xff {
			r[i]++
			return r[:i+1]
		}
	}
	return nil
}

// ContractTrunkKeyRanges returns the two CommitmentDomain key ranges that
// together cover every branch node of a contract's storage subtree.
//
// The commitment domain keys branches by HexToCompact(nibblePath); HexToCompact's
// flag byte differs by the parity of the path length, so a contract's storage
// branches (path = keccak256(addr)'s 64 nibbles ++ k slot-path nibbles, total
// 64+k) split into two non-adjacent byte ranges:
//   - even total length (k even, incl. the depth-64 subtree root):
//     key = 0x00 || H || <k/2 packed slot bytes>  ⇒  prefix 0x00||H (33 bytes)
//   - odd total length (k odd):
//     all such keys lie in [HexToCompact(H||0), NextSubtree(HexToCompact(H||15))).
func ContractTrunkKeyRanges(contractNibbles []byte) (evenFrom, evenTo, oddFrom, oddTo []byte) {
	evenFrom = nibbles.HexToCompact(contractNibbles) // 0x00 || H, 33 bytes
	evenTo = NextSubtree(evenFrom)

	odd0 := make([]byte, 0, len(contractNibbles)+1)
	odd0 = append(append(odd0, contractNibbles...), 0)
	oddF := make([]byte, 0, len(contractNibbles)+1)
	oddF = append(append(oddF, contractNibbles...), 15)
	oddFrom = nibbles.HexToCompact(odd0)
	oddTo = NextSubtree(nibbles.HexToCompact(oddF))
	return evenFrom, evenTo, oddFrom, oddTo
}

// ContractNibbles expands a 32-byte hash to its 64-nibble path (high nibble first).
func ContractNibbles(contractHash []byte) []byte {
	out := make([]byte, len(contractHash)*2)
	for i, b := range contractHash {
		out[2*i] = b >> 4
		out[2*i+1] = b & 0x0f
	}
	return out
}
