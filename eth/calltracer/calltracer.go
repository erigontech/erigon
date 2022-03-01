package calltracer

import (
	"encoding/binary"
	"math/big"
	"sort"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/log/v3"
)

type CallTracer struct {
	froms   map[common.Address]struct{}
	tos     map[common.Address]bool // address -> isCreated
	hasTEVM func(contractHash common.Hash) (bool, error)
}

func NewCallTracer(hasTEVM func(contractHash common.Hash) (bool, error)) *CallTracer {
	return &CallTracer{
		froms:   make(map[common.Address]struct{}),
		tos:     make(map[common.Address]bool),
		hasTEVM: hasTEVM,
	}
}

func (ct *CallTracer) CaptureStart(evm *vm.EVM, depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) {
	ct.froms[from] = struct{}{}

	created, ok := ct.tos[to]
	if !ok {
		ct.tos[to] = false
	}

	if !created && create {
		if len(code) > 0 && ct.hasTEVM != nil {
			has, err := ct.hasTEVM(common.BytesToHash(crypto.Keccak256(code)))
			if !has {
				ct.tos[to] = true
			}

			if err != nil {
				log.Warn("while CaptureStart", "err", err)
			}
		}
	}
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) {
}
func (ct *CallTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	ct.froms[from] = struct{}{}
	ct.tos[to] = false
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

func (ct *CallTracer) WriteToDb(tx kv.RwTx, block *types.Block, vmConfig vm.Config) error {
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
	var prev common.Address
	var created bool
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
		// TEVM marking still untranslated contracts
		if vmConfig.EnableTEMV {
			if created = ct.tos[addr]; created {
				v[length.Addr] |= 4
			}
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
