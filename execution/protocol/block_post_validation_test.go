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

package protocol

import (
	"math/big"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// TestBlockPostValidation_PreByzantiumBloomMismatch covers the regression
// where a pre-Byzantium block with an invalid logs bloom was silently
// accepted because the bloom check was gated on the same Byzantium flag as
// the receipt-root check (hive bcInvalidHeaderTest/log1_wrongBloom_Frontier
// and friends).
func TestBlockPostValidation_PreByzantiumBloomMismatch(t *testing.T) {
	t.Parallel()

	// Frontier rules — pre-Byzantium, so the receipt-root path is not exercised.
	cfg := &chain.Config{ChainID: big.NewInt(1)}

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

	err := BlockPostValidation(21912, 0, checkReceipts, checkBloom, receipts, header, nil, cfg, log.New())
	if err == nil {
		t.Fatal("expected bloom-mismatch error on pre-Byzantium block, got nil")
	}
	if !strings.Contains(err.Error(), "invalid bloom") {
		t.Fatalf("expected \"invalid bloom\" error, got: %v", err)
	}

	// Set the correct bloom — validation must pass.
	header.Bloom = correctBloom
	if err := BlockPostValidation(21912, 0, checkReceipts, checkBloom, receipts, header, nil, cfg, log.New()); err != nil {
		t.Fatalf("expected success when bloom matches, got: %v", err)
	}

	// With checkBloom disabled the mismatch must not trigger an error
	// (sanity check that the new flag does gate the new path).
	header.Bloom = types.Bloom{}
	if err := BlockPostValidation(21912, 0, checkReceipts, false, receipts, header, nil, cfg, log.New()); err != nil {
		t.Fatalf("checkBloom=false should skip bloom validation, got: %v", err)
	}
}
