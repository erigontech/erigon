package evmonego

/*
#include "evmonego.h"
*/
import "C"
import (
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

var emptyCodeHash = crypto.Keccak256Hash(nil)

type ExecEnv interface {
	// GetEnvMessage() Message
	GetEnvEVM() *vm.EVM
	GetEnvIntraBlockState() evmtypes.IntraBlockState
}

type HostImpl struct {
	// env      ExecEnv
	evm      *vm.EVM
	ibs      evmtypes.IntraBlockState
	txCtx    evmtypes.TxContext
	blockCtx evmtypes.BlockContext
	config   vm.Config
	chainID  *big.Int
	handle   *C.struct_evmc_vm
	rev      Revision
	bailout  bool
}

func NewEvmOneHost(env ExecEnv, bailout bool) *HostImpl {
	handle := C.new_evmc_vm()
	rules := env.GetEnvEVM().ChainRules()
	var rev Revision
	switch {
	case rules.IsPrague:
		rev = Prague
	case rules.IsCancun:
		rev = Cancun
	case rules.IsShanghai:
		rev = Shanghai
	case rules.IsLondon:
		rev = London
	case rules.IsBerlin:
		rev = Berlin
	case rules.IsIstanbul:
		rev = Istanbul
	case rules.IsPetersburg:
		rev = Petersburg
	case rules.IsConstantinople:
		rev = Constantinople
	case rules.IsByzantium:
		rev = Byzantium
	case rules.IsSpuriousDragon:
		rev = SpuriousDragon
	case rules.IsTangerineWhistle:
		rev = TangerineWhistle
	case rules.IsHomestead:
		rev = Homestead
	default: // TODO add all forks
		rev = Frontier
	}
	evm := env.GetEnvEVM()
	ibs := env.GetEnvIntraBlockState()
	txCtx := env.GetEnvEVM().TxContext
	blockCtx := env.GetEnvEVM().Context
	config := evm.Config()
	chainID := rules.ChainID
	h := &HostImpl{evm, ibs, txCtx, blockCtx, config, chainID, handle, rev, bailout}
	return h
}

func (h *HostImpl) AccountExists(addr common.Address) bool {
	r := h.ibs.Exist(common.Address(addr))
	return r
}

func (h *HostImpl) GetStorage(addr common.Address, key common.Hash) common.Hash {
	w := new(uint256.Int)
	h.ibs.GetState(addr, &key, w)
	return w.Bytes32()
}
func (h *HostImpl) SetStorage(addr common.Address, key common.Hash, value common.Hash) StorageStatus {
	var (
		current  uint256.Int
		original uint256.Int
		_value   uint256.Int
		status   = StorageAssigned
	)
	_value.SetBytes32(value[:])

	h.ibs.GetState(addr, &key, &current)
	h.ibs.GetCommittedState(addr, &key, &original)

	dirty := !original.Eq(&current)
	restored := original.Eq(&_value)
	currentIsZero := current.IsZero()
	valueIsZero := _value.IsZero()

	if !dirty && !restored {
		if currentIsZero {
			status = StorageAdded
		} else if valueIsZero {
			status = StorageDeleted
		} else {
			status = StorageModified
		}
	} else if dirty && !restored {
		if currentIsZero && !valueIsZero {
			status = StorageDeletedAdded
		} else if !currentIsZero && valueIsZero {
			status = StorageModifiedDeleted
		}
	} else if dirty && restored {
		if currentIsZero {
			status = StorageDeletedRestored
		} else if valueIsZero {
			status = StorageAddedDeleted
		} else {
			status = StorageModifiedRestored
		}
	}

	h.ibs.SetState(addr, &key, _value)
	return status
}
func (h *HostImpl) GetBalance(addr common.Address) common.Hash {
	return h.ibs.GetBalance(addr).Bytes32()
}
func (h *HostImpl) GetCodeSize(addr common.Address) int {
	return h.ibs.GetCodeSize(addr)
}
func (h *HostImpl) GetCodeHash(addr common.Address) common.Hash {
	return h.ibs.GetCodeHash(addr)
}
func (h *HostImpl) GetCode(addr common.Address) []byte {
	return h.ibs.GetCode(addr)
}
func (h *HostImpl) Selfdestruct(addr common.Address, beneficiary common.Address) bool {
	balance := *h.ibs.GetBalance(addr)
	if h.rev >= Cancun {
		h.ibs.SubBalance(addr, &balance, tracing.BalanceDecreaseSelfdestruct)
		h.ibs.AddBalance(beneficiary, &balance, tracing.BalanceIncreaseSelfdestruct)
		h.ibs.Selfdestruct6780(addr)
		r := h.ibs.HasSelfdestructed(addr)
		return r
	}
	h.ibs.AddBalance(beneficiary, &balance, tracing.BalanceIncreaseSelfdestruct)
	h.ibs.Selfdestruct(addr)
	return h.ibs.HasSelfdestructed(addr)
}
func (h *HostImpl) GetTxContext() TxContext {
	chainID := new(uint256.Int)
	chainID.SetFromBig(h.chainID)
	hash := chainID.Bytes32()
	var blobBaseFee, randao common.Hash
	if h.blockCtx.BlobBaseFee != nil {
		blobBaseFee = h.blockCtx.BlobBaseFee.Bytes32()
	}
	if h.blockCtx.PrevRanDao != nil {
		randao = *h.blockCtx.PrevRanDao
	} else {
		// TODO: assert rev >= Paris
		x := new(uint256.Int)
		x.SetFromBig(h.blockCtx.Difficulty)
		randao = x.Bytes32()
	}
	return TxContext{
		GasPrice:    h.txCtx.GasPrice.Bytes32(),
		Origin:      h.txCtx.Origin,
		Coinbase:    h.blockCtx.Coinbase,
		Number:      int64(h.blockCtx.BlockNumber),
		Timestamp:   int64(h.blockCtx.Time),
		GasLimit:    int64(h.blockCtx.GasLimit),
		PrevRandao:  randao,
		ChainID:     common.Hash(hash),
		BaseFee:     h.blockCtx.BaseFee.Bytes32(),
		BlobBaseFee: blobBaseFee,
	}
}
func (h *HostImpl) GetBlockHash(number int64) common.Hash {
	return h.blockCtx.GetHash(uint64(number))
}
func (h *HostImpl) EmitLog(addr common.Address, topics []common.Hash, data []byte) {
	h.ibs.AddLog(&types.Log{
		Address:     addr,
		Topics:      topics,
		Data:        data,
		BlockNumber: h.blockCtx.BlockNumber,
	})
}

func (h *HostImpl) handleCalls(kind CallKind,
	recipient common.Address,
	sender common.Address,
	value *uint256.Int,
	input []byte,
	gas uint64,
	depth int,
	static bool,
	salt common.Hash,
	codeAddress common.Address) (output []byte, gasLeft int64, gasRefund int64,
	createAddr common.Address, err error) {

	gasLeft = int64(gas)

	if kind == Call || kind == CallCode {
		// Fail if we're trying to transfer more than the available balance
		if !value.IsZero() && !h.evm.Context.CanTransfer(h.ibs, sender, value) {
			if !h.bailout {
				return nil, gasLeft, 0, common.Address{}, vm.ErrInsufficientBalance
			}
		}
	}

	p, isPrecompile := h.evm.Precompile(codeAddress)
	var code []byte
	if !isPrecompile {
		code = h.ibs.GetCode(codeAddress)
	}

	snapshot := h.ibs.Snapshot()

	if kind == Call {
		if !h.ibs.Exist(recipient) {
			if !isPrecompile && h.evm.ChainRules().IsSpuriousDragon && value.IsZero() {
				return nil, gasLeft, 0, common.Address{}, nil
			}
			h.ibs.CreateAccount(recipient, false)
		}
		h.evm.Context.Transfer(h.ibs, sender, recipient, value, h.bailout)
	}

	// else if kind == STATICCALL {
	// 	// We do an AddBalance of zero here, just in order to trigger a touch.
	// 	// This doesn't matter on Mainnet, where all empties are gone at the time of Byzantium,
	// 	// but is the correct thing to do and matters on other networks, in tests, and potential
	// 	// future scenarios
	// h.ibs.AddBalance(recipient, u256.Num0, tracing.BalanceChangeTouchAccount)
	// }

	var evmone_result Result
	var gLeft uint64
	// It is allowed to call precompiles, even via delegatecall
	if isPrecompile {
		output, gLeft, err = vm.RunPrecompiledContract(p, input, gas)
		gasLeft = int64(gLeft)
	} else if len(code) == 0 {
		// If the account has no code, we can abort here
		// The depth-check is already done, and precompiles handled above
		output, err = nil, nil // gas is unchanged
	} else {

		evmone_result, err = h.Execute(Call, static, depth, int64(gas), recipient, sender, input, value.Bytes32(), code)
		output = evmone_result.Output
		gasLeft = evmone_result.GasLeft
		gasRefund = evmone_result.GasRefund
	}
	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || h.config.RestoreState {
		h.ibs.RevertToSnapshot(snapshot)
		if err != vm.ErrExecutionReverted {
			gas = 0
		}
		// TODO: consider clearing up unused snapshots:
		//} else {
		//	evm.StateDB.DiscardSnapshot(snapshot)
	}

	return
}

func (h *HostImpl) Call(kind CallKind,
	recipient common.Address,
	sender common.Address,
	value common.Hash,
	input []byte,
	gas int64,
	depth int,
	static bool,
	salt common.Hash,
	codeAddress common.Address) (output []byte, gasLeft int64, gasRefund int64,
	createAddr common.Address, err error) {

	// fmt.Println("--")
	// fmt.Println("kind: ", kind)
	// fmt.Printf("recipient: 0x%x\n", recipient)
	// fmt.Printf("sender: 0x%x\n", sender)
	// fmt.Printf("value: 0x%x\n", value)
	// fmt.Printf("input: 0x%x\n", input)
	// fmt.Println("gas: ", gas)
	// fmt.Println("depth: ", depth)
	// fmt.Println("static: ", static)
	// fmt.Printf("salt: 0x%x\n", salt)
	// fmt.Printf("codeAddress: 0x%x\n", codeAddress)

	_value := new(uint256.Int).SetBytes32(value[:])

	if kind == Call || kind == DelegateCall || kind == CallCode {
		return h.handleCalls(kind, recipient, sender, _value, input, uint64(gas), depth, static, salt, codeAddress)
	}

	var code []byte
	if kind == Create {
		createAddr = crypto.CreateAddress(sender, h.ibs.GetNonce(sender))
		code = input
	} else if kind == Create2 {
		createAddr = crypto.CreateAddress2(sender, salt, crypto.Keccak256Hash(input).Bytes())
		code = input
	} else if kind == EofCreate {
		// TODO
		// recipient = crypto.CreateEOFAddress(sender, salt, input)
	}
	recipient = createAddr

	var ret []byte

	if !h.evm.Context.CanTransfer(h.ibs, sender, _value) {
		err = vm.ErrInsufficientBalance
		return nil, 0, 0, common.Address{}, err
	}
	nonce := h.ibs.GetNonce(sender)
	if nonce+1 < nonce {
		err = vm.ErrNonceUintOverflow
		return nil, 0, 0, common.Address{}, err
	}
	h.ibs.SetNonce(sender, nonce+1)

	// We add this to the access list _before_ taking a snapshot. Even if the creation fails,
	// the access-list change should not be rolled back
	if h.evm.ChainRules().IsBerlin {
		h.ibs.AddAddressToAccessList(recipient)
	}
	// Ensure there's no existing contract already at the designated address
	contractHash := h.ibs.GetCodeHash(recipient)
	if h.ibs.GetNonce(recipient) != 0 || (contractHash != (common.Hash{}) && contractHash != emptyCodeHash) {
		err = vm.ErrContractAddressCollision
		return nil, 0, 0, common.Address{}, err
	}
	// Create a new account on the state
	snapshot := h.ibs.Snapshot()
	h.ibs.CreateAccount(recipient, true)
	if h.evm.ChainRules().IsSpuriousDragon {
		h.ibs.SetNonce(recipient, 1)
	}
	h.evm.Context.Transfer(h.ibs, sender, recipient, _value, false /* bailout */)

	var evmone_result Result
	evmone_result, err = h.Execute(kind, static, depth, gas, recipient, sender, input, value, code)
	ret = evmone_result.Output
	gasLeft = evmone_result.GasLeft
	gasRefund = evmone_result.GasRefund

	// EIP-170: Contract code size limit
	if err == nil && h.evm.ChainRules().IsSpuriousDragon && len(ret) > params.MaxCodeSize {
		// Gnosis Chain prior to Shanghai didn't have EIP-170 enabled,
		// but EIP-3860 (part of Shanghai) requires EIP-170.
		if !h.evm.ChainRules().IsAura || h.config.HasEip3860(h.evm.ChainRules()) {
			err = vm.ErrMaxCodeSizeExceeded
		}
	}

	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && h.evm.ChainRules().IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
		err = vm.ErrInvalidCode
	}

	if err == nil {
		createDataGas := uint64(len(ret)) * params.CreateDataGas
		if gasLeft >= int64(createDataGas) {
			gasLeft -= int64(createDataGas)
			h.ibs.SetCode(recipient, ret)
		} else if h.evm.ChainRules().IsHomestead {
			err = vm.ErrCodeStoreOutOfGas
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in homestead this also counts for code storage gas errors.
	if err != nil && (h.evm.ChainRules().IsHomestead || err != vm.ErrCodeStoreOutOfGas) {
		h.ibs.RevertToSnapshot(snapshot)
		if err != vm.ErrExecutionReverted {
			gasLeft, gasRefund = 0, 0
		}
	}
	codeAddress = recipient
	return ret, gasLeft, gasRefund, codeAddress, err
}

func (h *HostImpl) AccessAccount(addr common.Address) AccessStatus {
	addrMod := h.ibs.AddAddressToAccessList(addr)
	if addrMod {
		return ColdAccess
	}
	return WarmAccess
}
func (h *HostImpl) AccessStorage(addr common.Address, key common.Hash) AccessStatus {
	_, slotMod := h.ibs.AddSlotToAccessList(addr, key)
	if slotMod {
		return ColdAccess
	}
	return WarmAccess
}
func (h *HostImpl) GetTransientStorage(addr common.Address, key common.Hash) common.Hash {
	w := h.ibs.GetTransientState(addr, key)
	return w.Bytes32()
}
func (h *HostImpl) SetTransientStorage(addr common.Address, key common.Hash, value common.Hash) {
	w := new(uint256.Int)
	w = w.SetBytes(value[:])
	h.ibs.SetTransientState(addr, key, *w)
}
