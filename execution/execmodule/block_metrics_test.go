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

package execmodule_test

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

type jsonLogCollector struct {
	mu   sync.Mutex
	msgs []string
	next log.Handler
}

func (c *jsonLogCollector) Log(r *log.Record) error {
	if strings.HasPrefix(r.Msg, `{"level"`) {
		c.mu.Lock()
		c.msgs = append(c.msgs, r.Msg)
		c.mu.Unlock()
	}
	if c.next != nil {
		return c.next.Log(r)
	}
	return nil
}

func (c *jsonLogCollector) Enabled(context.Context, log.Lvl) bool { return true }

func installCollector(t *testing.T) *jsonLogCollector {
	t.Helper()
	prev := log.Root().GetHandler()
	c := &jsonLogCollector{next: prev}
	log.Root().SetHandler(c)
	t.Cleanup(func() { log.Root().SetHandler(prev) })
	return c
}

func (c *jsonLogCollector) records(t *testing.T) []map[string]any {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]map[string]any, 0, len(c.msgs))
	for _, m := range c.msgs {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(m), &rec))
		out = append(out, rec)
	}
	return out
}

func TestSlowBlockMetricsAreEmittedForValidatedBlocks(t *testing.T) {
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	prevReadMetrics := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prevReadMetrics })
	dbg.EnableKVReadLevelledMetrics()

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithSlowBlockThreshold(0),
	)

	// New installs a root handler at LvlError, replacing any earlier collector.
	collector := installCollector(t)

	chainResult, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chainResult.Blocks))

	records := collector.records(t)
	require.Len(t, records, len(chainResult.Blocks),
		"exactly one record per block: a second emission site would double every block")

	var sawStateHash, sawAccountReads bool
	for _, rec := range records {
		block := rec["block"].(map[string]any)
		assert.NotZero(t, block["number"], "block number must be filled in")
		assert.NotEmpty(t, block["hash"])

		timing := rec["timing"].(map[string]any)
		require.Contains(t, timing, "execution_ms")
		require.Contains(t, timing, "state_hash_ms")
		assert.GreaterOrEqual(t, timing["total_ms"].(float64), timing["state_hash_ms"].(float64))
		if timing["state_hash_ms"].(float64) > 0 {
			sawStateHash = true
		}

		reads, hasReads := rec["state_reads"].(map[string]any)
		require.True(t, hasReads, "state_reads must be present once the counters are on")
		require.Contains(t, rec, "cache")
		if reads["accounts"].(float64) > 0 {
			sawAccountReads = true
		}
	}

	assert.True(t, sawStateHash, "state_hash_ms was zero in every record — commitment timing is not reaching the emitter")
	assert.True(t, sawAccountReads, "state_reads.accounts was zero in every record — the counters are not reaching the emitter")
}

func TestSlowBlockMetricsSilentByDefault(t *testing.T) {
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	collector := installCollector(t)

	chainResult, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chainResult.Blocks))

	assert.Empty(t, collector.records(t), "metrics must stay off unless the threshold is set")
}

func TestSlowBlockMetricsSkipMultiBlockForkValidation(t *testing.T) {
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithSlowBlockThreshold(0),
	)

	send := func(value uint64) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			txn, err := types.SignTx(
				types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(value), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
				*types.LatestSignerForChainID(nil), privKey,
			)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	}

	canonical, err := m.GenerateChain(1, send(1_000))
	require.NoError(t, err)
	fork, err := m.GenerateChainFrom(m.Genesis, 2, send(2_000))
	require.NoError(t, err)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))

	collector := installCollector(t)

	status, err := m.InsertBlocks(t.Context(), fork.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	tip := fork.Blocks[len(fork.Blocks)-1].Header()
	result, err := m.ValidateChain(t.Context(), tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	_, err = m.UpdateForkChoice(t.Context(), tip)
	require.NoError(t, err)

	assert.Empty(t, collector.records(t),
		"a multi-block fork validation must not be reported as one block")
}
