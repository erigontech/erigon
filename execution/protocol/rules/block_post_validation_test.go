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

package rules

import (
	"context"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type msgCaptureHandler struct{ msgs *[]string }

func (h msgCaptureHandler) Log(r *log.Record) error {
	*h.msgs = append(*h.msgs, r.Msg)
	return nil
}

func (h msgCaptureHandler) Enabled(context.Context, log.Lvl) bool { return true }

func captureLogger(msgs *[]string) log.Logger {
	l := log.New()
	l.SetHandler(msgCaptureHandler{msgs: msgs})
	return l
}

func countMsg(msgs []string, want string) int {
	n := 0
	for _, m := range msgs {
		if m == want {
			n++
		}
	}
	return n
}

// TestBlockPostValidation_PreByzantiumBloomMismatch covers the regression
// where a pre-Byzantium block with an invalid logs bloom was silently
// accepted because the bloom check was gated on the same Byzantium flag as
// the receipt-root check (hive bcInvalidHeaderTest/log1_wrongBloom_Frontier
// and friends).
func TestBlockPostValidation_PreByzantiumBloomMismatch(t *testing.T) {
	t.Parallel()

	// Frontier rules — pre-Byzantium, so the receipt-root path is not exercised.
	cfg := &chain.Config{ChainID: uint256.NewInt(1)}

	logAddr := common.HexToAddress("0x095e7baea6a6c7c4c2dfeb977efac326af552d87")
	receipt := &types.Receipt{
		CumulativeGasUsed: 21912,
		Logs: []*types.Log{{
			Address: logAddr,
			Topics:  []common.Hash{},
			Data:    []byte{},
		}},
	}
	receipts := types.Receipts{receipt}
	correctBloom := types.CreateBloom(receipts)
	if correctBloom == (types.Bloom{}) {
		t.Fatal("test setup: a non-empty log should produce a non-zero bloom")
	}

	header := &types.Header{
		Number:  *uint256.NewInt(1),
		GasUsed: 21912,
		// Bloom intentionally left zero — header disagrees with logs.
	}

	const checkReceipts = false // pre-Byzantium gate
	const checkBloom = true

	err := DefaultBlockPostValidation(cfg, header, 21912, 0, checkReceipts, checkBloom, receipts, nil, log.New())
	if err == nil {
		t.Fatal("expected bloom-mismatch error on pre-Byzantium block, got nil")
	}
	if !strings.Contains(err.Error(), "invalid bloom") {
		t.Fatalf("expected \"invalid bloom\" error, got: %v", err)
	}

	// Set the correct bloom — validation must pass.
	header.Bloom = correctBloom
	if err := DefaultBlockPostValidation(cfg, header, 21912, 0, checkReceipts, checkBloom, receipts, nil, log.New()); err != nil {
		t.Fatalf("expected success when bloom matches, got: %v", err)
	}

	// With checkBloom disabled the mismatch must not trigger an error
	// (sanity check that the new flag does gate the new path).
	header.Bloom = types.Bloom{}
	if err := DefaultBlockPostValidation(cfg, header, 21912, 0, checkReceipts, false, receipts, nil, log.New()); err != nil {
		t.Fatalf("checkBloom=false should skip bloom validation, got: %v", err)
	}
}

func TestBlockPostValidation_ReceiptBloomReuse(t *testing.T) {
	t.Parallel()

	cfg := &chain.Config{ChainID: uint256.NewInt(1)}
	receipts := types.Receipts{
		{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 21_000,
			Logs: []*types.Log{
				{
					Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
					Topics:  []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")},
				},
				{
					Address: common.HexToAddress("0x2222222222222222222222222222222222222222"),
					Topics:  []common.Hash{common.HexToHash("0x03")},
				},
			},
		},
		{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 42_000,
			Logs: []*types.Log{
				{
					Address: common.HexToAddress("0x3333333333333333333333333333333333333333"),
					Topics:  []common.Hash{common.HexToHash("0x04")},
				},
			},
		},
	}
	expectedBloom := types.CreateBloom(receipts)
	if expectedBloom == (types.Bloom{}) {
		t.Fatal("test setup: non-empty logs should produce a non-zero bloom")
	}
	for _, receipt := range receipts {
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	}
	receiptHash := types.DeriveSha(receipts)
	for _, receipt := range receipts {
		receipt.Bloom = types.Bloom{}
	}

	header := &types.Header{
		Number:      *uint256.NewInt(4_370_000),
		GasUsed:     42_000,
		ReceiptHash: receiptHash,
		Bloom:       expectedBloom,
	}

	const checkReceipts = true
	const checkBloom = true

	if err := DefaultBlockPostValidation(cfg, header, 42_000, 0, checkReceipts, checkBloom, receipts, nil, log.New()); err != nil {
		t.Fatalf("expected receipt+bloom validation to accept OR-merged bloom: %v", err)
	}

	header.Bloom = types.Bloom{}
	err := DefaultBlockPostValidation(cfg, header, 42_000, 0, checkReceipts, checkBloom, receipts, nil, log.New())
	if err == nil {
		t.Fatal("expected bloom-mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid bloom") {
		t.Fatalf("expected \"invalid bloom\" error, got: %v", err)
	}
}

// TestBlockPostValidation_GasMismatchNoPerTxSpam pins that a gas-used mismatch
// is reported as a single summary line plus the error, with no per-transaction
// dump — the detailed receipt breakdown is left to the caller's
// LOG_HASH_MISMATCH_REASON path.
func TestBlockPostValidation_GasMismatchNoPerTxSpam(t *testing.T) {
	t.Parallel()

	cfg := &chain.Config{ChainID: uint256.NewInt(1)}
	receipts := types.Receipts{
		{Status: types.ReceiptStatusSuccessful, GasUsed: 21_000, CumulativeGasUsed: 21_000},
		{Status: types.ReceiptStatusSuccessful, GasUsed: 21_000, CumulativeGasUsed: 42_000},
		{Status: types.ReceiptStatusSuccessful, GasUsed: 21_000, CumulativeGasUsed: 63_000},
	}
	header := &types.Header{Number: *uint256.NewInt(1), GasUsed: 100_000}
	const execGas = 63_000 // != header.GasUsed, triggers the mismatch branch

	var msgs []string
	err := DefaultBlockPostValidation(cfg, header, execGas, 0, false, false, receipts, nil, captureLogger(&msgs))
	if err == nil {
		t.Fatal("expected gas-used mismatch error, got nil")
	}
	if got := countMsg(msgs, "gas used mismatch"); got != 1 {
		t.Fatalf("expected exactly one summary line, got %d", got)
	}
	if got := countMsg(msgs, "  tx gas detail"); got != 0 {
		t.Fatalf("gas mismatch must not emit a per-tx dump, got %d line(s)", got)
	}
}
