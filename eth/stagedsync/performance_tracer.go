package stagedsync

import (
	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
)

type ExeuctionPerformanceCounters struct {
	NumTx     uint64
	NumCall   uint64
	NumOp     uint64
	NumSstore uint64
	NumSload  uint64
}

func (c *ExeuctionPerformanceCounters) Add(other *ExeuctionPerformanceCounters) {
	c.NumTx += other.NumTx
	c.NumCall += other.NumCall
	c.NumOp += other.NumOp
	c.NumSstore += other.NumSstore
	c.NumSload += other.NumSload
}

func (c *ExeuctionPerformanceCounters) Reset() {
	c.NumTx = 0
	c.NumCall = 0
	c.NumOp = 0
	c.NumSstore = 0
	c.NumSload = 0
}

type PerformanceTracer struct {
	counters *ExeuctionPerformanceCounters
	backend  *calltracer.CallTracer
}

var _ vm.EVMLogger = (*PerformanceTracer)(nil)

func NewPerformanceTracer(counters *ExeuctionPerformanceCounters, backend *calltracer.CallTracer) *PerformanceTracer {
	return &PerformanceTracer{
		counters: counters,
		backend:  backend,
	}
}

// Transaction level
func (t *PerformanceTracer) CaptureTxStart(gasLimit uint64) {
	t.counters.NumTx++
	t.backend.CaptureTxStart(gasLimit)
}

func (t *PerformanceTracer) CaptureTxEnd(restGas uint64) {
	t.backend.CaptureTxEnd(restGas)
}

// Top call frame
func (t *PerformanceTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address,
	precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.counters.NumCall++
	t.backend.CaptureStart(env, from, to, precompile, create, input, gas, value, code)
}

func (t *PerformanceTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	t.backend.CaptureEnd(output, usedGas, err)
}

// Rest of the frames
func (t *PerformanceTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool,
	create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.counters.NumCall++
	t.backend.CaptureEnter(typ, from, to, precompile, create, input, gas, value, code)
}

func (t *PerformanceTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	t.backend.CaptureExit(output, usedGas, err)
}

// Opcode level
func (t *PerformanceTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext,
	rData []byte, depth int, err error) {
	t.counters.NumOp++
	if op == vm.SSTORE {
		t.counters.NumSstore++
	} else if op == vm.SLOAD {
		t.counters.NumSload++
	}
	t.backend.CaptureState(pc, op, gas, cost, scope, rData, depth, err)
}

func (t *PerformanceTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext,
	depth int, err error) {
	t.backend.CaptureFault(pc, op, gas, cost, scope, depth, err)
}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *PerformanceTracer) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {
	t.backend.CapturePreimage(pc, hash, preimage)
}

func (t *PerformanceTracer) WriteToDb(tx kv.StatelessWriteTx, block *types.Block, vmConfig vm.Config) error {
	return t.backend.WriteToDb(tx, block, vmConfig)
}
