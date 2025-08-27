// Copyright 2022 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package native

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/types"
)

//go:generate gencodec -type account -field-override accountMarshaling -out gen_account_json.go

func init() {
	register("prestateTracer", newPrestateTracer)
}

type state = map[common.Address]*account

type account struct {
	Balance *big.Int                    `json:"balance,omitempty"`
	Code    []byte                      `json:"code,omitempty"`
	Nonce   uint64                      `json:"nonce,omitempty"`
	Storage map[common.Hash]common.Hash `json:"storage,omitempty"`
}

func (a *account) exists() bool {
	return a.Nonce > 0 || len(a.Code) > 0 || len(a.Storage) > 0 || (a.Balance != nil && a.Balance.Sign() != 0)
}

type accountMarshaling struct {
	Balance *hexutil.Big
	Code    hexutil.Bytes
}

type prestateTracer struct {
	env       *tracing.VMContext
	pre       state
	post      state
	create    bool
	to        common.Address
	gasLimit  uint64 // Amount of gas bought for the whole tx
	config    prestateTracerConfig
	interrupt uint32 // Atomic flag to signal execution interruption
	reason    error  // Textual reason for the interruption
	created   map[common.Address]bool
	deleted   map[common.Address]bool
}

type prestateTracerConfig struct {
	DiffMode       bool `json:"diffMode"`       // If true, this tracer will return state modifications
	DisableCode    bool `json:"disableCode"`    // If true, this tracer will not return the contract code
	DisableStorage bool `json:"disableStorage"` // If true, this tracer will not return the contract storage
}

func newPrestateTracer(ctx *tracers.Context, cfg json.RawMessage) (*tracers.Tracer, error) {
	var config prestateTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, err
		}
	}
	t := &prestateTracer{
		pre:     state{},
		post:    state{},
		config:  config,
		created: make(map[common.Address]bool),
		deleted: make(map[common.Address]bool),
	}

	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: t.OnTxStart,
			OnTxEnd:   t.OnTxEnd,
			OnOpcode:  t.OnOpcode,
		},
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}, nil
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (t *prestateTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if t.config.DiffMode {
		return
	}

	if t.create {
		// Keep existing account prior to contract creation at that address
		if s := t.pre[t.to]; s != nil && !s.exists() {
			// Exclude newly created contract.
			delete(t.pre, t.to)
		}
	}
}

// OnOpcode implements the EVMLogger interface to trace a single step of VM execution.
func (t *prestateTracer) OnOpcode(pc uint64, opcode byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	op := vm.OpCode(opcode)
	stackData := scope.StackData()
	stackLen := len(stackData)
	caller := scope.Address()
	switch {
	case stackLen >= 1 && (op == vm.SLOAD || op == vm.SSTORE):
		slot := common.Hash(stackData[stackLen-1].Bytes32())
		t.lookupStorage(caller, slot)
	case stackLen >= 1 && (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT):
		addr := common.Address(stackData[stackLen-1].Bytes20())
		t.lookupAccount(addr)
		if op == vm.SELFDESTRUCT {
			t.deleted[caller] = true
		}
	case stackLen >= 5 && (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE):
		addr := common.Address(stackData[stackLen-2].Bytes20())
		t.lookupAccount(addr)
	case op == vm.CREATE:
		nonce, _ := t.env.IntraBlockState.GetNonce(caller)
		addr := types.CreateAddress(caller, nonce)
		t.lookupAccount(addr)
		t.created[addr] = true
	case stackLen >= 4 && op == vm.CREATE2:
		offset := stackData[stackLen-2]
		size := stackData[stackLen-3]
		init, err := tracers.GetMemoryCopyPadded(scope.MemoryData(), int64(offset.Uint64()), int64(size.Uint64()))
		if err != nil {
			t.Stop(fmt.Errorf("failed to copy CREATE2 in prestate tracer input err: %s", err))
			return
		}
		inithash := crypto.Keccak256(init)
		salt := stackData[stackLen-4]
		addr := types.CreateAddress2(caller, salt.Bytes32(), inithash)
		t.lookupAccount(addr)
		t.created[addr] = true
	}
}

func (t *prestateTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	t.env = env

	nounce, _ := env.IntraBlockState.GetNonce(from)

	if tx.GetTo() == nil {
		t.create = true
		t.to = types.CreateAddress(from, nounce)
	} else {
		t.to = *tx.GetTo()
		t.create = false
	}

	t.lookupAccount(from)
	t.lookupAccount(t.to)
	t.lookupAccount(env.Coinbase)

	// Add accounts with authorizations to the prestate before they get applied.
	var b [32]byte
	data := bytes.NewBuffer(nil)
	for _, auth := range tx.GetAuthorizations() {
		data.Reset()
		addr, err := auth.RecoverSigner(data, b[:])
		if err != nil {
			continue
		}
		t.lookupAccount(*addr)
	}

	if t.create && t.config.DiffMode {
		t.created[t.to] = true
	}
}

func (t *prestateTracer) OnTxEnd(receipt *types.Receipt, err error) {
	if !t.config.DiffMode {
		return
	}

	for addr, state := range t.pre {
		// The deleted account's state is pruned from `post` but kept in `pre`
		if _, ok := t.deleted[addr]; ok {
			continue
		}
		modified := false
		postAccount := &account{Storage: make(map[common.Hash]common.Hash)}
		newBalance, _ := t.env.IntraBlockState.GetBalance(addr)
		newNonce, _ := t.env.IntraBlockState.GetNonce(addr)

		if newBalance.ToBig().Cmp(t.pre[addr].Balance) != 0 {
			modified = true
			postAccount.Balance = newBalance.ToBig()
		}
		if newNonce != t.pre[addr].Nonce {
			modified = true
			postAccount.Nonce = newNonce
		}

		if !t.config.DisableCode {
			newCode, _ := t.env.IntraBlockState.GetCode(addr)
			if !bytes.Equal(newCode, t.pre[addr].Code) {
				modified = true
				postAccount.Code = newCode
			}
		}

		if !t.config.DisableStorage {
			for key, val := range state.Storage {
				// don't include the empty slot
				if val == (common.Hash{}) {
					delete(t.pre[addr].Storage, key)
				}

				var newVal uint256.Int
				t.env.IntraBlockState.GetState(addr, key, &newVal)
				if new(uint256.Int).SetBytes(val[:]).Eq(&newVal) {
					// Omit unchanged slots
					delete(t.pre[addr].Storage, key)
				} else {
					modified = true
					if !newVal.IsZero() {
						postAccount.Storage[key] = newVal.Bytes32()
					}
				}
			}
		}

		if modified {
			t.post[addr] = postAccount
		} else {
			// if state is not modified, then no need to include into the pre state
			delete(t.pre, addr)
		}
	}
	// the new created contracts' prestate were empty, so delete them
	for a := range t.created {
		// the created contract maybe exists in statedb before the creating tx
		if s := t.pre[a]; s != nil && !s.exists() {
			delete(t.pre, a)
		}
	}
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *prestateTracer) GetResult() (json.RawMessage, error) {
	var res []byte
	var err error
	if t.config.DiffMode {
		res, err = json.Marshal(struct {
			Post state `json:"post"`
			Pre  state `json:"pre"`
		}{t.post, t.pre})
	} else {
		res, err = json.Marshal(t.pre)
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(res), t.reason
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *prestateTracer) Stop(err error) {
	t.reason = err
	atomic.StoreUint32(&t.interrupt, 1)
}

// lookupAccount fetches details of an account and adds it to the prestate
// if it doesn't exist there.
func (t *prestateTracer) lookupAccount(addr common.Address) {
	if _, ok := t.pre[addr]; ok {
		return
	}

	balance, _ := t.env.IntraBlockState.GetBalance(addr)
	nonce, _ := t.env.IntraBlockState.GetNonce(addr)
	code, _ := t.env.IntraBlockState.GetCode(addr)

	t.pre[addr] = &account{
		Balance: balance.ToBig(),
		Nonce:   nonce,
	}

	if !t.config.DisableCode {
		t.pre[addr].Code = code
	}
	if !t.config.DisableStorage {
		t.pre[addr].Storage = make(map[common.Hash]common.Hash)
	}
}

// lookupStorage fetches the requested storage slot and adds
// it to the prestate of the given contract. It assumes `lookupAccount`
// has been performed on the contract before.
func (t *prestateTracer) lookupStorage(addr common.Address, key common.Hash) {
	if t.config.DisableStorage {
		return
	}

	if _, ok := t.pre[addr].Storage[key]; ok {
		return
	}
	var val uint256.Int
	t.env.IntraBlockState.GetState(addr, key, &val)
	t.pre[addr].Storage[key] = val.Bytes32()
}
