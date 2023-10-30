package live

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func init() {
	register("livePrinter", newPrinter)
}

type Printer struct{}

func newPrinter(ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	return &Printer{}, nil
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (p *Printer) CaptureStart(from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	fmt.Printf("CaptureStart: from=%v, to=%v, precompile=%v, create=%v, input=%s, gas=%v, value=%v, code=%v\n", from, to, precompile, create, hexutility.Bytes(input), gas, value, hexutility.Bytes(code))
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (p *Printer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	fmt.Printf("CaptureEnd: output=%s, gasUsed=%v, err=%v\n", hexutility.Bytes(output), gasUsed, err)
}

// CaptureState implements the EVMLogger interface to trace a single step of VM execution.
func (p *Printer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	//fmt.Printf("CaptureState: pc=%v, op=%v, gas=%v, cost=%v, scope=%v, rData=%v, depth=%v, err=%v\n", pc, op, gas, cost, scope, rData, depth, err)
}

// CaptureFault implements the EVMLogger interface to trace an execution fault.
func (p *Printer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, _ *vm.ScopeContext, depth int, err error) {
	fmt.Printf("CaptureFault: pc=%v, op=%v, gas=%v, cost=%v, depth=%v, err=%v\n", pc, op, gas, cost, depth, err)
}

// CaptureKeccakPreimage is called during the KECCAK256 opcode.
func (p *Printer) CaptureKeccakPreimage(hash libcommon.Hash, data []byte) {}

// CaptureEnter is called when EVM enters a new scope (via call, create or selfdestruct).
func (p *Printer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	fmt.Printf("CaptureEnter: typ=%v, from=%v, to=%v, precompile=%v, create=%v, input=%s, gas=%v, value=%v, code=%v\n", typ, from, to, precompile, create, hexutility.Bytes(input), gas, value, hexutility.Bytes(code))
}

// CaptureExit is called when EVM exits a scope, even if the scope didn't
// execute any code.
func (p *Printer) CaptureExit(output []byte, gasUsed uint64, err error) {
	fmt.Printf("CaptureExit: output=%s, gasUsed=%v, err=%v\n", hexutility.Bytes(output), gasUsed, err)
}

func (p *Printer) CaptureTxStart(env *vm.EVM, tx types.Transaction) {
	buf, err := json.Marshal(tx)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	fmt.Printf("CaptureTxStart: tx=%s\n", buf)

}

func (p *Printer) CaptureTxEnd(receipt *types.Receipt, err error) {
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

func (p *Printer) OnBlockStart(b *types.Block, td *big.Int, finalized, safe *types.Header) {
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

func (p *Printer) OnBalanceChange(a libcommon.Address, prev, new *uint256.Int, reason evmtypes.BalanceChangeReason) {
	fmt.Printf("OnBalanceChange: a=%v, prev=%v, new=%v\n", a, prev, new)
}

func (p *Printer) OnNonceChange(a libcommon.Address, prev, new uint64) {
	fmt.Printf("OnNonceChange: a=%v, prev=%v, new=%v\n", a, prev, new)
}

func (p *Printer) OnCodeChange(a libcommon.Address, prevCodeHash libcommon.Hash, prev []byte, codeHash libcommon.Hash, code []byte) {
	fmt.Printf("OnCodeChange: a=%v, prevCodeHash=%v, prev=%s, codeHash=%v, code=%s\n", a, prevCodeHash, hexutility.Bytes(prev), codeHash, hexutility.Bytes(code))
}

func (p *Printer) OnStorageChange(a libcommon.Address, k *libcommon.Hash, prev, new uint256.Int) {
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

func (p *Printer) OnNewAccount(a libcommon.Address) {
	fmt.Printf("OnNewAccount: a=%v\n", a)
}

func (p *Printer) OnGasChange(old, new uint64, reason vm.GasChangeReason) {
	fmt.Printf("OnGasChange: old=%v, new=%v, diff=%v\n", old, new, new-old)
}

func (p *Printer) GetResult() (json.RawMessage, error) {
	return json.RawMessage{}, nil
}

func (p *Printer) Stop(err error) {
}
