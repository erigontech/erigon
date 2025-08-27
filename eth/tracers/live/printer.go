package live

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func init() {
	register("livePrinter", newPrinter)
}

type Printer struct{}

func newPrinter(ctx *tracers.Context, cfg json.RawMessage) (*tracers.Tracer, error) {
	t := &Printer{}
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart:       t.OnTxStart,
			OnTxEnd:         t.OnTxEnd,
			OnEnter:         t.OnEnter,
			OnExit:          t.OnExit,
			OnOpcode:        t.OnOpcode,
			OnFault:         t.OnFault,
			OnGasChange:     t.OnGasChange,
			OnBalanceChange: t.OnBalanceChange,
			OnNonceChange:   t.OnNonceChange,
			OnCodeChange:    t.OnCodeChange,
			OnStorageChange: t.OnStorageChange,
			OnLog:           t.OnLog,
		},
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}, nil
}

// OnExit is called after the call finishes to finalize the tracing.
func (p *Printer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	fmt.Printf("OnExit: output=%s, gasUsed=%v, err=%v\n", hexutil.Bytes(output), gasUsed, err)
}

// OnOpcode implements the EVMLogger interface to trace a single step of VM execution.
func (p *Printer) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	fmt.Printf("OnOpcode: pc=%v, op=%v, gas=%v, cost=%v, scope=%v, rData=%v, depth=%v, err=%v\n", pc, op, gas, cost, scope, rData, depth, err)
}

// OnFault implements the EVMLogger interface to trace an execution fault.
func (p *Printer) OnFault(pc uint64, op byte, gas, cost uint64, _ tracing.OpContext, depth int, err error) {
	fmt.Printf("OnFault: pc=%v, op=%v, gas=%v, cost=%v, depth=%v, err=%v\n", pc, op, gas, cost, depth, err)
}

func (p *Printer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	fmt.Printf("CaptureEnter: depth=%v, typ=%v from=%v, to=%v, input=%s, gas=%v, value=%v\n", depth, typ, from, to, hexutil.Bytes(input), gas, value)
}

func (p *Printer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	buf, err := json.Marshal(tx)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	fmt.Printf("OnTxStart: tx=%s\n", buf)

}

func (p *Printer) OnTxEnd(receipt *types.Receipt, err error) {
	if err != nil {
		fmt.Printf("CaptureTxEnd err: %v\n", err)
		return
	}
	buf, err := json.Marshal(receipt)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	fmt.Printf("CaptureTxEnd: receipt=%s\n", buf)
}

func (p *Printer) OnBlockStart(b *types.Block, td *big.Int, finalized, safe *types.Header, chainConfig *chain.Config) {
	if finalized != nil && safe != nil {
		fmt.Printf("OnBlockStart: b=%v, td=%v, finalized=%v, safe=%v\n", b.NumberU64(), td, finalized.Number.Uint64(), safe.Number.Uint64())
	} else {
		fmt.Printf("OnBlockStart: b=%v, td=%v\n", b.NumberU64(), td)
	}
}

func (p *Printer) OnBlockEnd(err error) {
	fmt.Printf("OnBlockEnd: err=%v\n", err)
}

func (p *Printer) OnGenesisBlock(b *types.Block, alloc types.GenesisAlloc) {
	fmt.Printf("OnGenesisBlock: b=%v, allocLength=%d\n", b.NumberU64(), len(alloc))
}

func (p *Printer) OnBalanceChange(a common.Address, prev, new uint256.Int, reason tracing.BalanceChangeReason) {
	fmt.Printf("OnBalanceChange: a=%v, prev=%v, new=%v\n", a, prev, new)
}

func (p *Printer) OnNonceChange(a common.Address, prev, new uint64) {
	fmt.Printf("OnNonceChange: a=%v, prev=%v, new=%v\n", a, prev, new)
}

func (p *Printer) OnCodeChange(a common.Address, prevCodeHash common.Hash, prev []byte, codeHash common.Hash, code []byte) {
	fmt.Printf("OnCodeChange: a=%v, prevCodeHash=%v, prev=%s, codeHash=%v, code=%s\n", a, prevCodeHash, hexutil.Bytes(prev), codeHash, hexutil.Bytes(code))
}

func (p *Printer) OnStorageChange(a common.Address, k common.Hash, prev, new uint256.Int) {
	fmt.Printf("OnStorageChange: a=%v, k=%v, prev=%v, new=%v\n", a, k, prev, new)
}

func (p *Printer) OnLog(l *types.Log) {
	buf, err := json.Marshal(l)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	fmt.Printf("OnLog: l=%s\n", buf)
}

func (p *Printer) OnGasChange(old, new uint64, reason tracing.GasChangeReason) {
	fmt.Printf("OnGasChange: old=%v, new=%v, diff=%v\n", old, new, new-old)
}

func (p *Printer) GetResult() (json.RawMessage, error) {
	return json.RawMessage{}, nil
}

func (p *Printer) Stop(err error) {
}
