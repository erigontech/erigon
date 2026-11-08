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

package debugprint

import (
	"fmt"

	"github.com/erigontech/erigon/execution/types"
)

// nolint
func Headers(h1, h2 *types.Header) {
	fmt.Printf("==== Header ====\n")
	fmt.Printf("root:        %x, %x\n", h1.Root, h2.Root)
	fmt.Printf("nonce:       %d, %d\n", h1.Nonce.Uint64(), h2.Nonce.Uint64())
	fmt.Printf("number:      %d, %d\n", h1.Number.Uint64(), h2.Number.Uint64())
	fmt.Printf("gasLimit:    %d, %d\n", h1.GasLimit, h2.GasLimit)
	fmt.Printf("gasUsed:     %d, %d\n", h1.GasUsed, h2.GasUsed)
	fmt.Printf("Difficulty:  %d, %d\n", h1.Difficulty, h2.Difficulty)
	fmt.Printf("ReceiptHash: %x, %x\n", h1.ReceiptHash, h2.ReceiptHash)
	fmt.Printf("TxHash:      %x, %x\n", h1.TxHash, h2.TxHash)
	fmt.Printf("UncleHash:   %x, %x\n", h1.UncleHash, h2.UncleHash)
	fmt.Printf("ParentHash:  %x, %x\n", h1.ParentHash, h2.ParentHash)
}
