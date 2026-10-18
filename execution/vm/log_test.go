package vm

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestLogDataSurvivesMemoryAndArenaReuse(t *testing.T) {
	t.Parallel()
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	scope := &CallContext{Contract: *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})}
	op := makeLog(3)
	for i, size := range []int{1024 * 1024, 1024, 32, 0, 1024 * 1024} {
		ibs.SetTxContext(1, 0)
		scope.Memory.Resize(uint64(size + 6))
		data := bytes.Repeat([]byte{byte(i + 1)}, size)
		scope.Memory.Set(6, uint64(size), data)
		for range 3 {
			scope.Stack.push(uint256.Int{})
		}
		scope.Stack.push(*uint256.NewInt(uint64(size)))
		scope.Stack.push(*uint256.NewInt(6))
		_, _, err := op(0, evm, scope)
		require.NoError(t, err)
		clear(scope.Memory.Data())
		logs := ibs.GetRawLogs(0)
		require.Len(t, logs, 1)
		require.Equal(t, []common.Hash{{}, {}, {}}, logs[0].Topics)
		require.Len(t, logs[0].Data, size)
		require.True(t, bytes.Equal(data, logs[0].Data), "log data changed for size %d", size)
		ibs.Reset()
	}
}
