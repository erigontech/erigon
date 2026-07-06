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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// fakeAmendEngine implements only the rules.EngineReader methods
// NewEVMBlockContext calls when an explicit author is supplied, standing in
// for an L2 rules engine that populates BlockContext.L2Version.
type fakeAmendEngine struct {
	rules.EngineReader
	l2Version uint64
}

func (f fakeAmendEngine) GetTransferFunc() evmtypes.TransferFunc                 { return misc.Transfer }
func (f fakeAmendEngine) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc { return nil }
func (f fakeAmendEngine) GetStartTxFunc() evmtypes.StartTxFunc                   { return nil }
func (f fakeAmendEngine) GetGasChargingFunc() evmtypes.GasChargingFunc           { return nil }
func (f fakeAmendEngine) GetComputeRefundFunc() evmtypes.ComputeRefundFunc       { return nil }

func (f fakeAmendEngine) AmendBlockContext(bc *evmtypes.BlockContext, _ *types.Header) {
	bc.L2Version = f.l2Version
}

// fakeL2Config resolves Rules.L2Version straight from BlockContext.L2Version,
// mirroring the fork-oracle commit's evmtypes rules_test fake.
type fakeL2Config struct{}

func (fakeL2Config) Name() string { return "fakel2" }

func (fakeL2Config) ResolveRules(l2Version, _, _ uint64, r *chain.Rules) {
	r.L2Version = l2Version
}

// TestNewEVMBlockContext_AmendBlockContextReachesRules verifies that
// NewEVMBlockContext calls the engine's AmendBlockContext after building the
// context, and that the L2Version it sets flows through to Rules resolution.
func TestNewEVMBlockContext_AmendBlockContextReachesRules(t *testing.T) {
	t.Parallel()

	header := &types.Header{}
	author := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	engine := fakeAmendEngine{l2Version: 7}
	cfg := &chain.Config{L2: fakeL2Config{}}

	bc := NewEVMBlockContext(header, nil, engine, author, cfg)
	require.Equal(t, uint64(7), bc.L2Version)

	r := bc.Rules(cfg)
	require.Equal(t, uint64(7), r.L2Version)
}

// TestNewEVMBlockContext_NilEngineSkipsAmend verifies the nil-engine call
// sites (the majority of NewEVMBlockContext callers do not pass an L2
// engine) are unaffected: AmendBlockContext is simply not invoked.
func TestNewEVMBlockContext_NilEngineSkipsAmend(t *testing.T) {
	t.Parallel()

	header := &types.Header{}
	author := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	cfg := &chain.Config{}

	bc := NewEVMBlockContext(header, nil, nil, author, cfg)
	require.Zero(t, bc.L2Version)
}
