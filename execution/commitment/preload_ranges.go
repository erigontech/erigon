// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// Splits by parity: even path length ⟷ odd (see HexToCompact for the split).
func ContractTrunkKeyRanges(contractNibbles []byte) (evenFrom, evenTo, oddFrom, oddTo []byte) {
	evenFrom = nibbles.HexToCompact(contractNibbles) // 0x00 || H, 33 bytes
	evenTo, _ = kv.NextSubtree(evenFrom)

	odd0 := make([]byte, 0, len(contractNibbles)+1)
	odd0 = append(append(odd0, contractNibbles...), 0)
	oddF := make([]byte, 0, len(contractNibbles)+1)
	oddF = append(append(oddF, contractNibbles...), 15)
	oddFrom = nibbles.HexToCompact(odd0)
	oddTo, _ = kv.NextSubtree(nibbles.HexToCompact(oddF))
	return evenFrom, evenTo, oddFrom, oddTo
}

func ContractNibbles(contractHash []byte) []byte {
	n := len(contractHash) * 2
	return nibbles.KeybytesToHex(contractHash)[:n:n]
}
