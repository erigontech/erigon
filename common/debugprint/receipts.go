package debugprint

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/types"
)

func Receipts(rs1, rs2 types.Receipts) {
	fmt.Printf("==== Receips ====\n")
	fmt.Printf("len(Receipts): %d, %d\n", len(rs1), len(rs2))
	for len(rs2) < len(rs1) {
		rs2 = append(rs2, &types.Receipt{})
	}

	for i := range rs1 {
		r1, r2 := rs1[i], rs2[i]
		fmt.Printf(" ==== Receipt ====\n")
		fmt.Printf(" TxHash:            %x, %x\n", r1.TxHash, r2.TxHash)
		fmt.Printf(" PostState:         %x, %x\n", r1.PostState, r2.PostState)
		fmt.Printf(" Status:            %d, %d\n", r1.Status, r2.Status)
		fmt.Printf(" CumulativeGasUsed: %d, %d\n", r1.CumulativeGasUsed, r2.CumulativeGasUsed)
		fmt.Printf(" ContractAddress:   %x, %x\n", r1.ContractAddress, r2.ContractAddress)
		fmt.Printf(" GasUsed:           %x, %x\n", r1.GasUsed, r2.GasUsed)
		fmt.Printf(" len(Logs):         %d, %d\n", len(r1.Logs), len(r2.Logs))
		for len(r2.Logs) < len(r1.Logs) {
			r2.Logs = append(r2.Logs, &types.Log{})
		}
		for j := range r1.Logs {
			l1, l2 := r1.Logs[j], r2.Logs[j]
			fmt.Printf("  Logs[%d].Address: %x, %x\n", j, l1.Address, l2.Address)
			fmt.Printf("  Logs[%d].Topic:   %x, %x\n", j, l1.Topics, l2.Topics)
			fmt.Printf("  Logs[%d].Data:    %x, %x\n", j, l1.Data, l2.Data)
		}
		fmt.Printf(" Bloom: %x, %x\n", r1.Bloom, r1.Bloom)
	}
	return
}

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
