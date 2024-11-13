package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"sync/atomic"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/log/v3"
)

var emptyCodeHash = crypto.Keccak256(nil)

//go:generate go run github.com/fjl/gencodec -type account -field-override accountMarshaling -out gen_account_json.go

func init() {
	register("zeroTracer", newZeroTracer)
}

type zeroTracer struct {
	noopTracer  // stub struct to mock not used interface methods
	env         *vm.EVM
	tx          types.TxnInfo
	gasLimit    uint64      // Amount of gas bought for the whole tx
	interrupt   atomic.Bool // Atomic flag to signal execution interruption
	reason      error       // Textual reason for the interruption
	ctx         *tracers.Context
	to          *libcommon.Address
	txStatus    uint64
	addrOpCodes map[libcommon.Address]map[vm.OpCode]struct{}
}

func newZeroTracer(ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	return &zeroTracer{
		tx: types.TxnInfo{
			Traces: make(map[libcommon.Address]*types.TxnTrace),
		},
		ctx:         ctx,
		addrOpCodes: make(map[libcommon.Address]map[vm.OpCode]struct{}),
	}, nil
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (t *zeroTracer) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.to = &to
	t.env = env

	t.env.IntraBlockState().SetDisableBalanceInc(true)

	t.addAccountToTrace(from)
	t.addAccountToTrace(to)
	t.addAccountToTrace(env.Context.Coinbase)

	if code != nil {
		t.addOpCodeToAccount(to, vm.CALL)
	}

	for _, a := range t.ctx.Txn.GetAccessList() {
		t.addAccountToTrace(a.Address)

		for _, k := range a.StorageKeys {
			t.addSLOADToAccount(a.Address, k)
		}
	}

	// The recipient balance includes the value transferred.
	toBal := new(big.Int).Sub(t.tx.Traces[to].Balance.ToBig(), value.ToBig())
	t.tx.Traces[to].Balance = uint256.MustFromBig(toBal)

	// The sender balance is after reducing: value and gasLimit.
	// We need to re-add them to get the pre-tx balance.
	fromBal := new(big.Int).Set(t.tx.Traces[from].Balance.ToBig())
	gasPrice := env.TxContext.GasPrice
	consumedGas := new(big.Int).Mul(gasPrice.ToBig(), new(big.Int).SetUint64(t.gasLimit))
	fromBal.Add(fromBal, new(big.Int).Add(value.ToBig(), consumedGas))
	t.tx.Traces[from].Balance = uint256.MustFromBig(fromBal)
	if t.tx.Traces[from].Nonce.Cmp(uint256.NewInt(0)) > 0 {
		t.tx.Traces[from].Nonce.Sub(t.tx.Traces[from].Nonce, uint256.NewInt(1))
	}
}

func (t *zeroTracer) CaptureEnter(op vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.addAccountToTrace(from)
	t.addAccountToTrace(to)
	t.addOpCodeToAccount(to, op)
}

func (t *zeroTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

// CaptureState implements the EVMLogger interface to trace a single step of VM execution.
func (t *zeroTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}

	stack := scope.Stack
	stackData := stack.Data
	stackLen := len(stackData)
	caller := scope.Contract.Address()

	switch {
	case stackLen >= 1 && op == vm.SLOAD:
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		t.addAccountToTrace(caller)
		t.addSLOADToAccount(caller, slot)
	case stackLen >= 2 && op == vm.SSTORE:
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		t.addAccountToTrace(caller)
		t.addSSTOREToAccount(caller, slot, stackData[stackLen-2].Clone())
	case stackLen >= 1 && (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT):
		addr := libcommon.Address(stackData[stackLen-1].Bytes20())
		t.addAccountToTrace(addr)
		t.addOpCodeToAccount(addr, op)
	case stackLen >= 5 && (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE):
		addr := libcommon.Address(stackData[stackLen-2].Bytes20())
		t.addAccountToTrace(addr)
		t.addOpCodeToAccount(addr, op)
	case op == vm.CREATE:
		nonce := uint64(0)
		if t.env.IntraBlockState().HasLiveAccount(caller) {
			nonce = t.env.IntraBlockState().GetNonce(caller)
		}
		addr := crypto.CreateAddress(caller, nonce)
		t.addAccountToTrace(addr)
		t.addOpCodeToAccount(addr, op)
	case stackLen >= 4 && op == vm.CREATE2:
		offset := stackData[stackLen-2]
		size := stackData[stackLen-3]
		init, err := GetMemoryCopyPadded(scope.Memory, int64(offset.Uint64()), int64(size.Uint64()))
		if err != nil {
			log.Warn("failed to copy CREATE2 input", "err", err, "tracer", "prestateTracer", "offset", offset, "size", size)
			return
		}
		inithash := crypto.Keccak256(init)
		salt := stackData[stackLen-4]
		addr := crypto.CreateAddress2(caller, salt.Bytes32(), inithash)
		t.addAccountToTrace(addr)
		t.addOpCodeToAccount(addr, op)
	}
}

const (
	memoryPadLimit = 1024 * 1024
)

// GetMemoryCopyPadded returns offset + size as a new slice.
// It zero-pads the slice if it extends beyond memory bounds.
func GetMemoryCopyPadded(m *vm.Memory, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, errors.New("offset or size must not be negative")
	}
	if int(offset+size) < m.Len() { // slice fully inside memory
		return m.GetCopy(offset, size), nil
	}
	paddingNeeded := int(offset+size) - m.Len()
	if paddingNeeded > memoryPadLimit {
		return nil, fmt.Errorf("reached limit for padding memory slice: %d", paddingNeeded)
	}
	cpy := make([]byte, size)
	if overlap := int64(m.Len()) - offset; overlap > 0 {
		copy(cpy, m.GetPtr(offset, overlap))
	}
	return cpy, nil
}

func (t *zeroTracer) CaptureTxEnd(restGas uint64) {
	t.tx.Meta.GasUsed = t.gasLimit - restGas
	*t.ctx.CumulativeGasUsed += t.tx.Meta.GasUsed

	toDelete := make([]libcommon.Address, 0)
	for addr := range t.tx.Traces {
		// Check again if the account was accessed through IntraBlockState
		seenAccount := t.env.IntraBlockState().SeenAccount(addr)
		// If an account was never accessed through IntraBlockState, it means that never there was an OpCode that read into it or checks whether it exists in the state trie, and therefore we don't need the trace of it.
		if !seenAccount {
			toDelete = append(toDelete, addr)
			continue
		}

		trace := t.tx.Traces[addr]
		hasLiveAccount := t.env.IntraBlockState().HasLiveAccount(addr)
		newBalance := t.env.IntraBlockState().GetBalance(addr)
		newNonce := uint256.NewInt(t.env.IntraBlockState().GetNonce(addr))
		codeHash := t.env.IntraBlockState().GetCodeHash(addr)
		code := t.env.IntraBlockState().GetCode(addr)
		hasSelfDestructed := t.env.IntraBlockState().HasSelfdestructed(addr)

		if newBalance.Cmp(trace.Balance) != 0 {
			trace.Balance = newBalance.Clone()
		} else {
			trace.Balance = nil
		}

		if newNonce.Cmp(trace.Nonce) != 0 {
			trace.Nonce = newNonce.Clone()
		} else {
			trace.Nonce = nil
		}

		if len(trace.StorageReadMap) > 0 && hasLiveAccount {
			trace.StorageRead = make([]libcommon.Hash, 0, len(trace.StorageReadMap))
			for k := range trace.StorageReadMap {
				if t.env.IntraBlockState().HasLiveState(addr, &k) {
					trace.StorageRead = append(trace.StorageRead, k)
				}
			}
		} else {
			trace.StorageRead = nil
		}

		if len(trace.StorageWritten) == 0 || !hasLiveAccount || !t.env.IntraBlockState().IsDirtyJournal(addr) {
			trace.StorageWritten = nil
		} else {
			// A slot write could be reverted if the transaction is reverted. We will need to read the value from the statedb again to get the correct value.
			for k := range trace.StorageWritten {
				var value uint256.Int
				t.env.IntraBlockState().GetState(addr, &k, &value)
				trace.StorageWritten[k] = &value
			}
		}

		if !bytes.Equal(codeHash[:], emptyCodeHash) && !bytes.Equal(codeHash[:], trace.CodeUsage.Read[:]) {
			trace.CodeUsage.Read = nil
			trace.CodeUsage.Write = bytes.Clone(code)
		} else if code != nil {
			codeHashCopy := libcommon.BytesToHash(codeHash.Bytes())
			trace.CodeUsage.Read = &codeHashCopy
		}

		// When the code is empty for this account, we don't need to store the code usage
		if code == nil {
			trace.CodeUsage = nil
		}

		// if addr == libcommon.HexToAddress("0xed12310d5a37326e6506209c4838146950166760") {
		// 	fmt.Printf("Address: %s, opcodes: %v\n", addr.String(), t.addrOpCodes[addr])
		// }

		if trace.CodeUsage != nil && trace.CodeUsage.Read != nil {
			if t.addrOpCodes[addr] != nil {
				// We don't need to provide the actual bytecode UNLESS the opcode is the following:
				// DELEGATECALL, CALL, STATICCALL, CALLCODE, EXTCODECOPY, EXTCODEHASH, EXTCODESIZE
				opCodes := []vm.OpCode{vm.DELEGATECALL, vm.CALL, vm.STATICCALL, vm.CALLCODE, vm.EXTCODECOPY,
					vm.EXTCODEHASH, vm.EXTCODESIZE}
				keep := false
				for _, opCode := range opCodes {
					if _, ok := t.addrOpCodes[addr][opCode]; ok {
						keep = true
						break
					}
				}
				if !keep {
					trace.CodeUsage = nil
				}
			} else {
				trace.CodeUsage = nil
			}
		}

		if hasSelfDestructed {
			trace.SelfDestructed = new(bool)
			*trace.SelfDestructed = true
		}
	}

	for _, addr := range toDelete {
		delete(t.tx.Traces, addr)
	}

	receipt := &types.Receipt{Type: t.ctx.Txn.Type(), CumulativeGasUsed: *t.ctx.CumulativeGasUsed}
	receipt.Status = t.txStatus
	receipt.TxHash = t.ctx.Txn.Hash()
	receipt.GasUsed = t.tx.Meta.GasUsed

	// if the transaction created a contract, store the creation address in the receipt.
	if t.to == nil {
		receipt.ContractAddress = crypto.CreateAddress(t.env.TxContext.Origin, t.ctx.Txn.GetNonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = t.env.IntraBlockState().GetLogs(t.ctx.Txn.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockNumber = big.NewInt(0).SetUint64(t.ctx.BlockNum)
	receipt.TransactionIndex = uint(t.ctx.TxIndex)

	receiptBuffer := &bytes.Buffer{}
	encodeErr := receipt.EncodeRLP(receiptBuffer)

	if encodeErr != nil {
		log.Error("failed to encode receipt", "err", encodeErr)
		return
	}

	t.tx.Meta.NewReceiptTrieNode = receiptBuffer.Bytes()

	txBuffer := &bytes.Buffer{}
	encodeErr = t.ctx.Txn.MarshalBinary(txBuffer)

	if encodeErr != nil {
		log.Error("failed to encode transaction", "err", encodeErr)
		return
	}

	t.tx.Meta.ByteCode = txBuffer.Bytes()
}

func (t *zeroTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if err != nil {
		t.txStatus = types.ReceiptStatusFailed
	} else {
		t.txStatus = types.ReceiptStatusSuccessful
	}
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *zeroTracer) GetResult() (json.RawMessage, error) {
	var res []byte
	var err error
	res, err = json.Marshal(t.tx)

	if err != nil {
		return nil, err
	}

	return json.RawMessage(res), t.reason
}

func (t *zeroTracer) GetTxnInfo() types.TxnInfo {
	return t.tx
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *zeroTracer) Stop(err error) {
	t.reason = err
	t.interrupt.Store(true)
}

func (t *zeroTracer) addAccountToTrace(addr libcommon.Address) {
	if _, ok := t.tx.Traces[addr]; ok {
		return
	}

	balance := uint256.NewInt(0)
	nonce := uint256.NewInt(0)
	codeHash := libcommon.Hash{}

	if t.env.IntraBlockState().HasLiveAccount(addr) {
		nonce = uint256.NewInt(t.env.IntraBlockState().GetNonce(addr))
		balance = t.env.IntraBlockState().GetBalance(addr)
		codeHash = t.env.IntraBlockState().GetCodeHash(addr)
	}

	t.tx.Traces[addr] = &types.TxnTrace{
		Balance:        balance.Clone(),
		Nonce:          nonce.Clone(),
		CodeUsage:      &types.ContractCodeUsage{Read: &codeHash},
		StorageWritten: make(map[libcommon.Hash]*uint256.Int),
		StorageRead:    make([]libcommon.Hash, 0),
		StorageReadMap: make(map[libcommon.Hash]struct{}),
	}
}

func (t *zeroTracer) addSLOADToAccount(addr libcommon.Address, key libcommon.Hash) {
	t.tx.Traces[addr].StorageReadMap[key] = struct{}{}
	t.addOpCodeToAccount(addr, vm.SLOAD)
}

func (t *zeroTracer) addSSTOREToAccount(addr libcommon.Address, key libcommon.Hash, value *uint256.Int) {
	t.tx.Traces[addr].StorageWritten[key] = value
	t.tx.Traces[addr].StorageReadMap[key] = struct{}{}
	t.addOpCodeToAccount(addr, vm.SSTORE)
}

func (t *zeroTracer) addOpCodeToAccount(addr libcommon.Address, op vm.OpCode) {
	if t.addrOpCodes[addr] == nil {
		t.addrOpCodes[addr] = make(map[vm.OpCode]struct{})
	}
	t.addrOpCodes[addr][op] = struct{}{}
}
