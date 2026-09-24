package vm_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// sloadAddSstore is `slot[0] += 1234`: PUSH1 0, SLOAD, PUSH2 1234, ADD, PUSH1 0, SSTORE.
const sloadAddSstore = "0x6000546104d201600055"

// The three PUSHes and the ADD of sloadAddSstore, plus the cold SLOAD that warms
// the slot before SSTORE reprices it.
const sloadAddSstoreOverhead = 4*3 + params.ColdSloadCostEIP2929 + params.WarmStorageReadCostEIP2929

var stale = uint256.MustFromHex("0xf6a7831804efd2cd0a")

func TestSStoreOriginalValueSource(t *testing.T) {
	address := accounts.InternAddress(common.BytesToAddress([]byte("contract")))

	for _, tt := range []struct {
		name      string
		committed *uint256.Int
		// setup runs on the execution-time state, after the committed state is sealed.
		setup    func(t *testing.T, s *state.IntraBlockState)
		wantUsed uint64
	}{
		{
			// SSTORE prices against a non-zero original equal to the current value:
			// 100 warm access + 2900 reset.
			name:      "stale value committed",
			committed: stale,
			wantUsed:  sloadAddSstoreOverhead + params.SstoreWriteExistingEIP2929,
		},
		{
			// A pre-transaction write makes the EVM read the stale value but leaves
			// the committed original at zero, so SSTORE takes the dirty-update
			// branch at 100 gas.
			name: "stale value written before the transaction",
			setup: func(t *testing.T, s *state.IntraBlockState) {
				require.NoError(t, s.SetState(address, accounts.ZeroKey, *stale))
			},
			wantUsed: sloadAddSstoreOverhead,
		},
		{
			// An engine-supplied baseline makes the stale value the transaction's
			// committed original, which is how both reference clients price it.
			name: "stale value installed as the committed baseline",
			setup: func(t *testing.T, s *state.IntraBlockState) {
				s.SetStorageBaseline(address, accounts.ZeroKey, *stale)
			},
			wantUsed: sloadAddSstoreOverhead + params.SstoreWriteExistingEIP2929,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tx, sd := testTemporalTxSD(t)
			txNum, _, err := sd.SeekCommitment(t.Context(), tx)
			require.NoError(t, err)
			getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
			r, w := state.NewReaderV3(getter), state.NewWriter(sd.AsPutDel(tx), nil, txNum)

			vmctx := evmtypes.BlockContext{
				CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
				Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
					return nil
				},
			}

			committed := state.New(r)
			require.NoError(t, committed.CreateAccount(address, true))
			require.NoError(t, committed.SetCode(address, hexutil.MustDecode(sloadAddSstore), tracing.CodeChangeUnspecified))
			if tt.committed != nil {
				require.NoError(t, committed.SetState(address, accounts.ZeroKey, *tt.committed))
			}
			require.NoError(t, committed.CommitBlock(vmctx.Rules(chain.TestChainBerlinConfig), w))
			committed.Close()

			s := state.New(r)
			defer s.Close()
			if tt.setup != nil {
				tt.setup(t, s)
			}

			vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.TestChainBerlinConfig, vm.Config{})
			const pool = 1_000_000
			_, gas, _, err := vmenv.Call(accounts.ZeroAddress, address, nil, mdgas.MdGas{Execution: pool}, uint256.Int{}, false)
			require.NoError(t, err)
			require.Equal(t, tt.wantUsed, pool-gas.Execution)

			got, err := s.GetState(address, accounts.ZeroKey)
			require.NoError(t, err)
			require.Equal(t, new(uint256.Int).AddUint64(stale, 1234).String(), got.String())
		})
	}
}
