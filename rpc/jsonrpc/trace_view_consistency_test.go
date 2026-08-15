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

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestTraceCallUsesCommittedState(t *testing.T) {
	m, bankAddress, contractAddress, _ := chainWithDeployedContract(t)

	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	publishedDomains, err := execctx.NewSharedDomains(m.Ctx, roTx, m.Log)
	require.NoError(t, err)
	defer publishedDomains.Close()

	storageKey := common.Hash{}
	compositeKey := make([]byte, 0, len(contractAddress)+len(storageKey))
	compositeKey = append(compositeKey, contractAddress[:]...)
	compositeKey = append(compositeKey, storageKey[:]...)
	require.NoError(t, publishedDomains.DomainPut(kv.StorageDomain, roTx, compositeKey, []byte{3}, 1, nil))

	stateCache := &execmodule.Cache{}
	stateCache.SetPublishedSD(func() *execctx.SharedDomains { return publishedDomains })
	base := newBaseApiForTest(m)
	base.stateCache = stateCache
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	input := hexutil.Bytes(crypto.Keccak256([]byte("retrieve()"))[:4])
	result, err := api.Call(m.Ctx, TraceCallParam{
		From: &bankAddress,
		To:   &contractAddress,
		Data: input,
	}, []string{TraceTypeTrace}, &latest, nil)
	require.NoError(t, err)

	expected := make(hexutil.Bytes, 32)
	expected[len(expected)-1] = 2
	require.Equal(t, expected, result.Output)
}

func TestTraceCallUsesCommittedHeader(t *testing.T) {
	base, m, _, events := newOverlayAheadTestAPIWithEvents(t)

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	committedHeader, err := m.BlockReader.HeaderByNumber(m.Ctx, tx, overlayRaceChainSize)
	require.NoError(t, err)
	require.NotNil(t, committedHeader)
	committedHash := committedHeader.Hash()

	overlayHeader := types.CopyHeader(committedHeader)
	overlayHeader.Coinbase = common.Address{2}
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, rawdb.WriteHeader(overlay, overlayHeader))
	require.NoError(t, rawdb.WriteCanonicalHash(overlay, overlayHeader.Hash(), overlayRaceChainSize))

	contractAddress := common.Address{3}
	coinbaseCode := hexutil.Bytes{byte(vm.COINBASE), 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	traceConfig := &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(contractAddress): {Code: &coinbaseCode},
		},
	}
	requestedBlock := rpc.BlockNumberOrHashWithHash(committedHash, true)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
	result, err := api.Call(m.Ctx, TraceCallParam{
		From: &m.Address,
		To:   &contractAddress,
	}, []string{TraceTypeTrace}, &requestedBlock, traceConfig)
	require.NoError(t, err)

	expected := make(hexutil.Bytes, 32)
	copy(expected[len(expected)-len(committedHeader.Coinbase):], committedHeader.Coinbase[:])
	require.Equal(t, expected, result.Output)
}
