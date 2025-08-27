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
func Transactions(ts1, ts2 types.Transactions) {
	fmt.Printf("==== Transactions ====\n")
	fmt.Printf("len(Transactions): %d, %d\n", len(ts1), len(ts2))
	for len(ts2) < len(ts1) {
		ts2 = append(ts2, nil)
	}
	for i := range ts1 {
		t1, t2 := ts1[i], ts2[i]
		fmt.Printf(" ==== Transaction ====\n")
		if t2 == nil {
			fmt.Printf(" TxHash:        %x\n", t1.Hash())
			fmt.Printf(" To:            %x\n", t1.GetTo())
			continue
		}
		fmt.Printf(" TxHash:        %x, %x\n", t1.Hash(), t2.Hash())
		fmt.Printf(" To:            %x, %x\n", t1.GetTo(), t2.GetTo())
	}
}

// nolint
func Receipts(rs1, rs2 types.Receipts) {
	fmt.Printf("==== Receipts ====\n")
	fmt.Printf("len(Receipts): %d, %d\n", len(rs1), len(rs2))
	for len(rs2) < len(rs1) {
		rs2 = append(rs2, nil)
	}

	for i := range rs1 {
		r1, r2 := rs1[i], rs2[i]
		fmt.Printf(" ==== Receipt ====\n")
		if r2 == nil {
			fmt.Printf(" TxHash:            %x\n", r1.TxHash)
			fmt.Printf(" PostState:         %x\n", r1.PostState)
			fmt.Printf(" Status:            %d\n", r1.Status)
			fmt.Printf(" CumulativeGasUsed: %d\n", r1.CumulativeGasUsed)
			fmt.Printf(" ContractAddress:   %x\n", r1.ContractAddress)
			fmt.Printf(" GasUsed:           %x\n", r1.GasUsed)
			fmt.Printf(" len(Logs):         %d\n", len(r1.Logs))
			continue
		}

		fmt.Printf(" TxHash:            %x, %x\n", r1.TxHash, r2.TxHash)
		fmt.Printf(" PostState:         %x, %x\n", r1.PostState, r2.PostState)
		fmt.Printf(" Status:            %d, %d\n", r1.Status, r2.Status)
		fmt.Printf(" CumulativeGasUsed: %d, %d\n", r1.CumulativeGasUsed, r2.CumulativeGasUsed)
		fmt.Printf(" ContractAddress:   %x, %x\n", r1.ContractAddress, r2.ContractAddress)
		fmt.Printf(" GasUsed:           %x, %x\n", r1.GasUsed, r2.GasUsed)
		fmt.Printf(" len(Logs):         %d, %d\n", len(r1.Logs), len(r2.Logs))
		for len(r2.Logs) < len(r1.Logs) {
			r2.Logs = append(r2.Logs, nil)
		}
		for j := range r1.Logs {
			l1 := r1.Logs[j]
			l2 := r2.Logs[j]
			if l2 == nil {
				fmt.Printf("  Logs[%d].Address: %x\n", j, l1.Address)
				fmt.Printf("  Logs[%d].Topic:   %x\n", j, l1.Topics)
				fmt.Printf("  Logs[%d].Data:    %x\n", j, l1.Data)
				continue
			}
			fmt.Printf("  Logs[%d].Address: %x, %x\n", j, l1.Address, l2.Address)
			fmt.Printf("  Logs[%d].Topic:   %x, %x\n", j, l1.Topics, l2.Topics)
			fmt.Printf("  Logs[%d].Data:    %x, %x\n", j, l1.Data, l2.Data)
		}
		//fmt.Printf(" Bloom: %x, %x\n", r1.Bloom, r1.Bloom)
	}
}

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
