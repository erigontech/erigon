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
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/rpc"
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
