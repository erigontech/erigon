package transactions

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func BenchmarkTraceTxAccountReads(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			sender := accounts.InternAddress(common.HexToAddress("0x1111"))
			recipient := accounts.InternAddress(common.HexToAddress("0x2222"))
			code := make([]byte, 0, count*5+1)
			for i := range count {
				code = append(code, byte(vm.PUSH2), byte((i+0x3000)>>8), byte(i+0x3000), byte(vm.BALANCE), byte(vm.POP))
			}
			code = append(code, byte(vm.STOP))
			ibs := state.NewPooled(state.NewNoopReader())
			defer state.ReleasePooled(ibs)
			msg := types.NewMessage(sender, recipient, 0, uint256.NewInt(0), 10_000_000,
				uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0), nil, nil, false, false, true, false, nil)
			txn := types.NewTransaction(0, recipient.Value(), uint256.NewInt(0), 10_000_000, uint256.NewInt(0), nil)
			blockCtx := evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer, GasLimit: 30_000_000}
			name := "callTracer"
			config := &tracersConfig.TraceConfig{Tracer: &name}
			b.ReportAllocs()
			for b.Loop() {
				ibs.Reset()
				VersionedCallState(ibs)
				if err := ibs.SetCode(recipient, code, tracing.CodeChangeUnspecified); err != nil {
					b.Fatal(err)
				}
				_, err := TraceTx(b.Context(), nil, txn, msg, blockCtx, protocol.NewEVMTxContext(msg),
					uint256.NewInt(0), common.Hash{}, 0, ibs, config, chain.TestChainOsakaConfig,
					jsonstream.New(io.Discard), time.Minute, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
