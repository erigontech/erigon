package blockreplay

import (
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type ancestorReader struct{ dbservices.FullBlockReader }

func (ancestorReader) CanonicalHash(_ context.Context, _ kv.Getter, n uint64) (common.Hash, bool, error) {
	return common.BigToHash(new(uint256.Int).SetUint64(n).ToBig()), true, nil
}

func TestCaptureAncestorsParentWindow(t *testing.T) {
	for _, n := range []uint64{1, 256, 257, 258, 300} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			fx := newFixture()
			err := captureAncestors(t.Context(), nil, ancestorReader{}, &types.Header{Number: *uint256.NewInt(n)}, fx)
			require.NoError(t, err)
			lo := uint64(0)
			if n > 257 {
				lo = n - 257
			}
			require.Equal(t, int(n-lo), len(fx.Ancestors))
			for i := lo; i < n; i++ {
				require.Contains(t, fx.Ancestors, i)
			}
		})
	}
}

type replayHashEngine struct {
	rules.Engine
	contract accounts.Address
}

func (e replayHashEngine) Initialize(_ *chain.Config, _ rules.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState, syscall rules.SysCallCustom, _ log.Logger, _ *tracing.Hooks) error {
	_, err := syscall(e.contract, nil, ibs, header, false)
	return err
}

func TestReplayMissingAncestor(t *testing.T) {
	fx := newFixture()
	contract := common.HexToAddress("0xc0de")
	code := []byte{byte(vm.PUSH1), 2, byte(vm.NUMBER), byte(vm.SUB), byte(vm.BLOCKHASH), byte(vm.STOP)}
	account := accounts.NewAccount()
	account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	fx.Accounts[contract] = toAcctData(&account)
	fx.Code[contract] = code
	parent := &types.Header{Number: *uint256.NewInt(2)}
	block := types.NewBlock(&types.Header{Number: *uint256.NewInt(3), ParentHash: parent.Hash(), GasLimit: 10_000_000}, nil, nil, nil, nil, nil)
	var err error
	fx.BlockRLP, err = rlpEncode(block)
	require.NoError(t, err)
	fx.ParentHeaderRLP, err = rlpEncodeHeader(parent)
	require.NoError(t, err)
	_, err = Replay(fx, chain.TestChainBerlinConfig, replayHashEngine{Engine: ethash.NewFaker(), contract: accounts.InternAddress(contract)}, 0, log.New())
	require.ErrorContains(t, err, "ancestor 1")
}
