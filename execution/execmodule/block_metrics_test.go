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
	"crypto/ecdsa"
	"encoding/json"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

func newMetricsTester(t *testing.T, opts ...execmoduletester.Option) (*execmoduletester.ExecModuleTester, *ecdsa.PrivateKey, common.Address) {
	t.Helper()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	opts = append([]execmoduletester.Option{
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	}, opts...)
	return execmoduletester.New(t, opts...), privKey, senderAddr
}

func sendTo(t *testing.T, m *execmoduletester.ExecModuleTester, privKey *ecdsa.PrivateKey, to common.Address, value uint64) func(int, *blockgen.BlockGen) {
	return func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(value), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil), privKey,
		)
		require.NoError(t, err)
		b.AddTx(txn)
	}
}

func sendThenDeploy(t *testing.T, m *execmoduletester.ExecModuleTester, privKey *ecdsa.PrivateKey, to common.Address, value uint64) func(int, *blockgen.BlockGen) {
	send := sendTo(t, m, privKey, to, value)
	return func(i int, b *blockgen.BlockGen) {
		if i == 0 {
			send(i, b)
			return
		}
		txn, err := types.SignTx(
			types.NewContractCreation(uint64(i), uint256.NewInt(0), 300_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), []byte{0x60, 0x01, 0x60, 0x00, 0xf3}),
			*types.LatestSignerForChainID(nil), privKey,
		)
		require.NoError(t, err)
		b.AddTx(txn)
	}
}

func TestSlowBlockMetricsAreEmittedForValidatedBlocks(t *testing.T) {
	prevReadMetrics := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prevReadMetrics })
	dbg.KVReadLevelledMetrics = false

	m, privKey, senderAddr := newMetricsTester(t, execmoduletester.WithSlowBlockThreshold(0))

	// New installs a root handler at LvlError, replacing any earlier collector.
	collector := installCollector(t)

	chainResult, err := m.GenerateChain(2, sendThenDeploy(t, m, privKey, senderAddr, 1_000))
	require.NoError(t, err)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chainResult.Blocks))

	records := collector.records(t)
	require.Len(t, records, len(chainResult.Blocks),
		"exactly one record per block: a second emission site would double every block")

	var sawAccountReads, sawCodeWrites bool
	for _, rec := range records {
		block := rec["block"].(map[string]any)
		assert.NotZero(t, block["number"], "block number must be filled in")
		assert.NotEmpty(t, block["hash"])

		timing := rec["timing"].(map[string]any)
		require.Contains(t, timing, "execution_ms")
		require.Contains(t, timing, "state_hash_ms")
		// state_hash_ms gets no lower bound: commitment over a test-sized block runs
		// in ~0.1ms whatever the block carries, below the Windows clock resolution.
		assert.GreaterOrEqual(t, timing["total_ms"].(float64), timing["state_hash_ms"].(float64))

		if block["gas_used"].(float64) > 0 {
			assert.Positive(t, timing["execution_ms"].(float64),
				"commitment is nested inside the validation span on the single-block path, so the subtraction never clamps")
			assert.Positive(t, rec["throughput"].(map[string]any)["mgas_per_sec"].(float64),
				"a zero rate here would be indistinguishable from a stalled block")
		}

		reads, hasReads := rec["state_reads"].(map[string]any)
		require.True(t, hasReads, "state_reads must be present once the counters are on")
		require.Contains(t, rec, "cache")
		if reads["accounts"].(float64) > 0 {
			sawAccountReads = true
		}

		writes := rec["state_writes"].(map[string]any)
		if code, ok := writes["code"].(float64); ok && code > 0 {
			sawCodeWrites = true
		}
	}

	assert.True(t, sawAccountReads, "state_reads.accounts was zero in every record — the counters are not reaching the emitter")
	assert.True(t, sawCodeWrites, "state_writes.code was zero across a contract deploy — the one code counter claimed real is not counted")
}

func TestSlowBlockThresholdRestoresReadMetrics(t *testing.T) {
	prevReadMetrics := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prevReadMetrics })
	dbg.KVReadLevelledMetrics = false

	t.Run("threshold set", func(t *testing.T) {
		newMetricsTester(t, execmoduletester.WithSlowBlockThreshold(0))
		require.True(t, dbg.KVReadLevelledMetrics,
			"the threshold must bring the counters with it, or state_reads silently vanishes")
	})

	require.False(t, dbg.KVReadLevelledMetrics,
		"read timing must not stay on for the rest of the test binary")
}

func TestSlowBlockMetricsSilentByDefault(t *testing.T) {
	m, privKey, senderAddr := newMetricsTester(t)
	collector := installCollector(t)

	chainResult, err := m.GenerateChain(1, sendTo(t, m, privKey, senderAddr, 1_000))
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chainResult.Blocks))

	assert.Empty(t, collector.records(t), "metrics must stay off unless the threshold is set")
}

func TestSlowBlockMetricsSkipReorgForkchoice(t *testing.T) {
	m, privKey, senderAddr := newMetricsTester(t, execmoduletester.WithSlowBlockThreshold(0))
	send := func(value uint64) func(int, *blockgen.BlockGen) {
		return sendTo(t, m, privKey, senderAddr, value)
	}

	canonical, err := m.GenerateChain(1, send(1_000))
	require.NoError(t, err)
	sideFork, err := m.GenerateChainFrom(m.Genesis, 1, send(2_000))
	require.NoError(t, err)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))

	collector := installCollector(t)

	status, err := m.InsertBlocks(t.Context(), sideFork.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	tip := sideFork.Blocks[len(sideFork.Blocks)-1].Header()
	result, err := m.ValidateChain(t.Context(), tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	_, err = m.UpdateForkChoice(t.Context(), tip)
	require.NoError(t, err)

	assert.Empty(t, collector.records(t),
		"a forkchoice that unwinds commits more than this block's writes, so commit_ms would not be this block's")
}

func TestSlowBlockMetricsSkipSupersededValidation(t *testing.T) {
	m, privKey, senderAddr := newMetricsTester(t, execmoduletester.WithSlowBlockThreshold(0))
	send := func(value uint64) func(int, *blockgen.BlockGen) {
		return sendTo(t, m, privKey, senderAddr, value)
	}

	siblingA, err := m.GenerateChainFrom(m.Genesis, 1, send(1_000))
	require.NoError(t, err)
	siblingB, err := m.GenerateChainFrom(m.Genesis, 1, send(2_000))
	require.NoError(t, err)

	status, err := m.InsertBlocks(t.Context(), siblingA.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	status, err = m.InsertBlocks(t.Context(), siblingB.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	tipA := siblingA.Blocks[len(siblingA.Blocks)-1].Header()
	tipB := siblingB.Blocks[len(siblingB.Blocks)-1].Header()

	// Both validate at the same height, so both leave a record; B validates
	// last, so the extending fork head is B when the forkchoice picks A.
	resultA, err := m.ValidateChain(t.Context(), tipA)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, resultA.ValidationStatus)
	resultB, err := m.ValidateChain(t.Context(), tipB)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, resultB.ValidationStatus)

	collector := installCollector(t)

	_, err = m.UpdateForkChoice(t.Context(), tipA)
	require.NoError(t, err)

	assert.Empty(t, collector.records(t),
		"A's state was discarded when B became the extending fork, so RunLoop re-executes A; the cached record describes the run that was thrown away")
}

func TestSlowBlockMetricsSkipMultiBlockForkValidation(t *testing.T) {
	m, privKey, senderAddr := newMetricsTester(t, execmoduletester.WithSlowBlockThreshold(0))
	send := func(value uint64) func(int, *blockgen.BlockGen) {
		return sendTo(t, m, privKey, senderAddr, value)
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
