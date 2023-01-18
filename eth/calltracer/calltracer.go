package calltracer

import (
	"encoding/binary"
	"sort"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
)

type CallTracer struct {
	froms map[libcommon.Address]struct{}
	tos   map[libcommon.Address]bool // address -> isCreated
}

func NewCallTracer() *CallTracer {
	return &CallTracer{
		froms: make(map[libcommon.Address]struct{}),
		tos:   make(map[libcommon.Address]bool),
	}
}

func (ct *CallTracer) CaptureTxStart(gasLimit uint64) {}
func (ct *CallTracer) CaptureTxEnd(restGas uint64)    {}

// CaptureStart and CaptureEnter also capture SELFDESTRUCT opcode invocations
func (ct *CallTracer) captureStartOrEnter(from, to libcommon.Address, create bool, code []byte) {
	ct.froms[from] = struct{}{}

	created, ok := ct.tos[to]
	if !ok {
		ct.tos[to] = false
	}

	if !created && create {
		if len(code) > 0 {
			ct.tos[to] = true
		}
	}
}

func (ct *CallTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.captureStartOrEnter(from, to, create, code)
}
func (ct *CallTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.captureStartOrEnter(from, to, create, code)
}
func (ct *CallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *CallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (ct *CallTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}
func (ct *CallTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}

func (ct *CallTracer) WriteToDb(tx kv.StatelessWriteTx, block *types.Block, vmConfig vm.Config) error {
	ct.tos[block.Coinbase()] = false
	for _, uncle := range block.Uncles() {
		ct.tos[uncle.Coinbase] = false
	}
	list := make(common.Addresses, len(ct.froms)+len(ct.tos))
	i := 0
	for addr := range ct.froms {
		copy(list[i][:], addr[:])
		i++
	}
	for addr := range ct.tos {
		copy(list[i][:], addr[:])
		i++
	}
	sort.Sort(list)
	// List may contain duplicates
	var blockNumEnc [8]byte
	binary.BigEndian.PutUint64(blockNumEnc[:], block.Number().Uint64())
	var prev libcommon.Address
	for j, addr := range list {
		if j > 0 && prev == addr {
			continue
		}
		var v [length.Addr + 1]byte
		copy(v[:], addr[:])
		if _, ok := ct.froms[addr]; ok {
			v[length.Addr] |= 1
		}
		if _, ok := ct.tos[addr]; ok {
			v[length.Addr] |= 2
		}
		if j == 0 {
			if err := tx.Append(kv.CallTraceSet, blockNumEnc[:], v[:]); err != nil {
				return err
			}
		} else {
			if err := tx.AppendDup(kv.CallTraceSet, blockNumEnc[:], v[:]); err != nil {
				return err
			}
		}
		copy(prev[:], addr[:])
	}
	return nil
}
