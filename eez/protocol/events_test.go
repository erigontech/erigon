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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/types"
)

// TestEventTopics recomputes each topic from the Solidity signature and pins
// the value observed on chain or listed in the network appendix.
func TestEventTopics(t *testing.T) {
	for _, c := range []struct {
		topic     common.Hash
		signature string
		pinned    string
	}{
		{BatchPostedTopic, "BatchPosted(bytes32,uint64[])", "0x70159c08b708a43c8e97ba2da7378cf59ee0b44dab6d764121b8079183ea477f"},
		{L2ExecutionPerformedTopic, "L2ExecutionPerformed(uint64,bytes32,uint256)", "0x8945ae7d1ab7f670bc04e5b87613a2ac6904a559769517d220308f4f22cd1b66"},
		{ExecutionConsumedTopic, "ExecutionConsumed(bytes32,uint64,uint256)", "0xa17dc82da628b280737819918ee433b966773121ff974c081d8cbac4c6199d7d"},
		{L2TxSkippedTopic, "L2TxSkipped(uint256,bytes)", "0x31dea3928d6f7bb415eb371434f719dfc337c078c82d58146f2b8a4726002e40"},
		{RollupCreatedTopic, "RollupCreated(uint64,address,bytes32)", "0x6867516efd826e92d08e1fdb96524601b1b494dc451db14c3f54299c071b583e"},
		{L2CrossChainCallExecutedTopic, "CrossChainCallExecuted(bytes32,address,address,bytes,uint256,uint64)", "0xec2129bc21aa3a28f7f705d280646ec2ebfbf1769b89938768c6bdbfb04da7c4"},
	} {
		require.Equal(t, crypto.Keccak256Hash([]byte(c.signature)), c.topic, c.signature)
		require.Equal(t, common.HexToHash(c.pinned), c.topic, c.signature)
	}
}

func chiadoLog(l *testfixtures.ChiadoLog) *types.Log {
	return &types.Log{Address: l.Address, Topics: l.Topics, Data: l.Data}
}

// TestParseChiadoLogs parses the logs of the live Chiado settlement and the
// registration of rollup 1.
func TestParseChiadoLogs(t *testing.T) {
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(t, err)
	var performed *L2ExecutionPerformed
	var posted *BatchPosted
	for i := range s.Logs {
		l := chiadoLog(&s.Logs[i])
		switch l.Topics[0] {
		case L2ExecutionPerformedTopic:
			performed, err = ParseL2ExecutionPerformed(l)
			require.NoError(t, err)
		case BatchPostedTopic:
			posted, err = ParseBatchPosted(l)
			require.NoError(t, err)
		default:
			_, err = ParseBatchPosted(l)
			require.ErrorIs(t, err, ErrUnexpectedEvent)
		}
	}
	require.NotNil(t, performed)
	require.Equal(t, uint64(1), performed.RollupID)
	require.Equal(t, s.StateAtBlock.RollupRoot, performed.NewRoot)
	require.Equal(t, 0, s.StateAtBlock.RollupEtherBalance.ToInt().Cmp(performed.EtherBalance))
	require.NotNil(t, posted)
	require.Equal(t, []uint64{1}, posted.RollupIDs)

	r, err := testfixtures.LoadChiadoRollupCreated()
	require.NoError(t, err)
	created, err := ParseRollupCreated(chiadoLog(&r.Log))
	require.NoError(t, err)
	require.Equal(t, uint64(1), created.RollupID)
	require.Equal(t, s.RollupManager, created.RollupContract)
	// The L2 genesis block hash, eth_getBlockByNumber(0) on the eez.dev L2.
	require.Equal(t, common.HexToHash("0x05227a1af51a988231b625b276c6b29594f0a93090efd93463934cd1bf6e606e"), created.InitialRoot)
}

// makeLog builds an ev log from its indexed topics and non-indexed values.
func makeLog(t *testing.T, ev *abi.Event, topics []common.Hash, values ...any) *types.Log {
	t.Helper()
	data, err := ev.Inputs.NonIndexed().Pack(values...)
	require.NoError(t, err)
	return &types.Log{Topics: append([]common.Hash{ev.ID}, topics...), Data: data}
}

func uint64Topic(v uint64) common.Hash { return common.BigToHash(new(big.Int).SetUint64(v)) }

func TestParseSyntheticLogs(t *testing.T) {
	g := newValueGen(5)
	callHash, root := g.hash(), g.hash()
	proxy, source := g.address(), g.address()
	value, index := g.uint256(), g.uint256()
	data := g.bytes()

	posted, err := ParseBatchPosted(makeLog(t, &batchPostedEvent, nil, root, []uint64{1, 1<<64 - 1}))
	require.NoError(t, err)
	require.Equal(t, &BatchPosted{SharedPublicInput: root, RollupIDs: []uint64{1, 1<<64 - 1}}, posted)

	performed, err := ParseL2ExecutionPerformed(makeLog(t, &l2ExecutionPerformedEvent, []common.Hash{uint64Topic(7)}, root, value))
	require.NoError(t, err)
	require.Equal(t, uint64(7), performed.RollupID)
	require.Equal(t, root, performed.NewRoot)
	require.Equal(t, 0, value.Cmp(performed.EtherBalance))

	consumed, err := ParseExecutionConsumed(makeLog(t, &executionConsumedEvent,
		[]common.Hash{callHash, uint64Topic(1<<64 - 1), common.BigToHash(index)}))
	require.NoError(t, err)
	require.Equal(t, callHash, consumed.CrossChainCallHash)
	require.Equal(t, uint64(1<<64-1), consumed.RollupID)
	require.Equal(t, 0, index.Cmp(consumed.EntryQueueIndex))

	skipped, err := ParseL2TxSkipped(makeLog(t, &l2TxSkippedEvent, []common.Hash{common.BigToHash(index)}, data))
	require.NoError(t, err)
	require.Equal(t, 0, index.Cmp(skipped.EntryIndex))
	require.Equal(t, data, skipped.RevertData)

	executed, err := ParseL2CrossChainCallExecuted(makeLog(t, &l2CrossChainCallExecutedEvent,
		[]common.Hash{callHash, common.BytesToHash(proxy[:])}, source, data, value, uint64(123456)))
	require.NoError(t, err)
	require.Equal(t, callHash, executed.CrossChainCallHash)
	require.Equal(t, proxy, executed.Proxy)
	require.Equal(t, source, executed.SourceAddress)
	require.Equal(t, data, executed.CallData)
	require.Equal(t, 0, value.Cmp(executed.Value))
	require.Equal(t, uint64(123456), executed.CallGas)
}

func TestParseRejects(t *testing.T) {
	root := common.HexToHash("0x1234")
	valid := func() *types.Log {
		return makeLog(t, &l2ExecutionPerformedEvent, []common.Hash{uint64Topic(1)}, root, big.NewInt(5))
	}

	_, err := ParseL2ExecutionPerformed(&types.Log{})
	require.ErrorIs(t, err, ErrUnexpectedEvent)
	_, err = ParseBatchPosted(valid())
	require.ErrorIs(t, err, ErrUnexpectedEvent)

	for name, mutate := range map[string]func(l *types.Log){
		"missing topic":  func(l *types.Log) { l.Topics = l.Topics[:1] },
		"extra topic":    func(l *types.Log) { l.Topics = append(l.Topics, root) },
		"dirty uint64":   func(l *types.Log) { l.Topics[1][0] = 1 },
		"trailing data":  func(l *types.Log) { l.Data = append(l.Data, 0) },
		"truncated data": func(l *types.Log) { l.Data = l.Data[:len(l.Data)-1] },
		"no data":        func(l *types.Log) { l.Data = nil },
	} {
		t.Run(name, func(t *testing.T) {
			l := valid()
			mutate(l)
			_, err := ParseL2ExecutionPerformed(l)
			require.ErrorIs(t, err, ErrMalformedLog)
		})
	}

	consumed := makeLog(t, &executionConsumedEvent, []common.Hash{root, uint64Topic(1), root})
	consumed.Data = []byte{0}
	_, err = ParseExecutionConsumed(consumed)
	require.ErrorIs(t, err, ErrMalformedLog)

	created := makeLog(t, &rollupCreatedEvent, []common.Hash{uint64Topic(1), root}, root)
	created.Topics[2][0] = 1
	_, err = ParseRollupCreated(created)
	require.ErrorIs(t, err, ErrMalformedLog)

	// A dynamic value whose offset skips a word decodes but is not canonical.
	skipped := makeLog(t, &l2TxSkippedEvent, []common.Hash{root}, []byte{0xaa})
	relocated := append(common.LeftPadBytes([]byte{0x40}, 32), make([]byte, 32)...)
	skipped.Data = append(relocated, skipped.Data[32:]...)
	_, err = ParseL2TxSkipped(skipped)
	require.ErrorIs(t, err, ErrMalformedLog)
}
