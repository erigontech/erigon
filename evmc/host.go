// EVMC: Ethereum Client-VM Connector API.
// Copyright 2018-2019 The EVMC Authors.
// Licensed under the Apache License, Version 2.0.

package evmc

/*
#cgo CFLAGS:  -I${SRCDIR}/.. -Wall -Wextra -Wno-unused-parameter

#include <evmc/evmc.h>
#include <evmc/helpers.h>

*/
import "C"
import (
	"math/big"
	"unsafe"

	"github.com/ledgerwatch/turbo-geth/common"
)

type CallKind int

const (
	Call         CallKind = C.EVMC_CALL
	DelegateCall CallKind = C.EVMC_DELEGATECALL
	CallCode     CallKind = C.EVMC_CALLCODE
	Create       CallKind = C.EVMC_CREATE
	Create2      CallKind = C.EVMC_CREATE2
)

type StorageStatus int

const (
	StorageUnchanged     StorageStatus = C.EVMC_STORAGE_UNCHANGED
	StorageModified      StorageStatus = C.EVMC_STORAGE_MODIFIED
	StorageModifiedAgain StorageStatus = C.EVMC_STORAGE_MODIFIED_AGAIN
	StorageAdded         StorageStatus = C.EVMC_STORAGE_ADDED
	StorageDeleted       StorageStatus = C.EVMC_STORAGE_DELETED
)

func goAddress(in C.evmc_address) common.Address {
	out := common.Address{}
	for i := 0; i < len(out); i++ {
		out[i] = byte(in.bytes[i])
	}
	return out
}

func goHash(in C.evmc_bytes32) common.Hash {
	out := common.Hash{}
	for i := 0; i < len(out); i++ {
		out[i] = byte(in.bytes[i])
	}
	return out
}

func goByteSlice(data *C.uint8_t, size C.size_t) []byte {
	if size == 0 {
		return []byte{}
	}
	return (*[1 << 30]byte)(unsafe.Pointer(data))[:size:size]
}

// TxContext contains information about current transaction and block.
type TxContext struct {
	GasPrice   common.Hash
	Origin     common.Address
	Coinbase   common.Address
	Number     int64
	Timestamp  int64
	GasLimit   int64
	Difficulty common.Hash
	ChainID    common.Hash
}

type HostContext interface {
	AccountExists(addr common.Address) bool
	GetStorage(addr common.Address, key common.Hash) common.Hash
	SetStorage(addr common.Address, key common.Hash, value common.Hash) StorageStatus
	GetBalance(addr common.Address) common.Hash
	GetCodeSize(addr common.Address) int
	GetCodeHash(addr common.Address) common.Hash
	GetCode(addr common.Address) []byte
	Selfdestruct(addr common.Address, beneficiary common.Address)
	GetTxContext() TxContext
	GetBlockHash(number int64) common.Hash
	EmitLog(addr common.Address, topics []common.Hash, data []byte)
	Call(kind CallKind,
		destination common.Address, sender common.Address, value *big.Int, input []byte, gas int64, depth int,
		static bool, salt *big.Int) (output []byte, gasLeft int64, createAddr common.Address, err error)
}

//export accountExists
func accountExists(pCtx unsafe.Pointer, pAddr *C.evmc_address) C.bool {
	ctx := getHostContext(uintptr(pCtx))
	return C.bool(ctx.AccountExists(goAddress(*pAddr)))
}

//export getStorage
func getStorage(pCtx unsafe.Pointer, pAddr *C.struct_evmc_address, pKey *C.evmc_bytes32) C.evmc_bytes32 {
	ctx := getHostContext(uintptr(pCtx))
	return evmcBytes32(ctx.GetStorage(goAddress(*pAddr), goHash(*pKey)))
}

//export setStorage
func setStorage(pCtx unsafe.Pointer, pAddr *C.evmc_address, pKey *C.evmc_bytes32, pVal *C.evmc_bytes32) C.enum_evmc_storage_status {
	ctx := getHostContext(uintptr(pCtx))
	return C.enum_evmc_storage_status(ctx.SetStorage(goAddress(*pAddr), goHash(*pKey), goHash(*pVal)))
}

//export getBalance
func getBalance(pCtx unsafe.Pointer, pAddr *C.evmc_address) C.evmc_uint256be {
	ctx := getHostContext(uintptr(pCtx))
	return evmcBytes32(ctx.GetBalance(goAddress(*pAddr)))
}

//export getCodeSize
func getCodeSize(pCtx unsafe.Pointer, pAddr *C.evmc_address) C.size_t {
	ctx := getHostContext(uintptr(pCtx))
	return C.size_t(ctx.GetCodeSize(goAddress(*pAddr)))
}

//export getCodeHash
func getCodeHash(pCtx unsafe.Pointer, pAddr *C.evmc_address) C.evmc_bytes32 {
	ctx := getHostContext(uintptr(pCtx))
	return evmcBytes32(ctx.GetCodeHash(goAddress(*pAddr)))
}

//export copyCode
func copyCode(pCtx unsafe.Pointer, pAddr *C.evmc_address, offset C.size_t, p *C.uint8_t, size C.size_t) C.size_t {
	ctx := getHostContext(uintptr(pCtx))
	code := ctx.GetCode(goAddress(*pAddr))
	length := C.size_t(len(code))

	if offset >= length {
		return 0
	}

	toCopy := length - offset
	if toCopy > size {
		toCopy = size
	}

	out := goByteSlice(p, size)
	copy(out, code[offset:])
	return toCopy
}

//export selfdestruct
func selfdestruct(pCtx unsafe.Pointer, pAddr *C.evmc_address, pBeneficiary *C.evmc_address) {
	ctx := getHostContext(uintptr(pCtx))
	ctx.Selfdestruct(goAddress(*pAddr), goAddress(*pBeneficiary))
}

//export getTxContext
func getTxContext(pCtx unsafe.Pointer) C.struct_evmc_tx_context {
	ctx := getHostContext(uintptr(pCtx))

	txContext := ctx.GetTxContext()

	return C.struct_evmc_tx_context{
		evmcBytes32(txContext.GasPrice),
		evmcAddress(txContext.Origin),
		evmcAddress(txContext.Coinbase),
		C.int64_t(txContext.Number),
		C.int64_t(txContext.Timestamp),
		C.int64_t(txContext.GasLimit),
		evmcBytes32(txContext.Difficulty),
		evmcBytes32(txContext.ChainID),
	}
}

//export getBlockHash
func getBlockHash(pCtx unsafe.Pointer, number int64) C.evmc_bytes32 {
	ctx := getHostContext(uintptr(pCtx))
	return evmcBytes32(ctx.GetBlockHash(number))
}

//export emitLog
func emitLog(pCtx unsafe.Pointer, pAddr *C.evmc_address, pData unsafe.Pointer, dataSize C.size_t, pTopics unsafe.Pointer, topicsCount C.size_t) {
	ctx := getHostContext(uintptr(pCtx))

	// FIXME: Optimize memory copy
	data := C.GoBytes(pData, C.int(dataSize))
	tData := C.GoBytes(pTopics, C.int(topicsCount*32))

	nTopics := int(topicsCount)
	topics := make([]common.Hash, nTopics)
	for i := 0; i < nTopics; i++ {
		copy(topics[i][:], tData[i*32:(i+1)*32])
	}

	ctx.EmitLog(goAddress(*pAddr), topics, data)
}

//export call
func call(pCtx unsafe.Pointer, msg *C.struct_evmc_message) C.struct_evmc_result {
	ctx := getHostContext(uintptr(pCtx))

	kind := CallKind(msg.kind)
	output, gasLeft, createAddr, err := ctx.Call(kind, goAddress(msg.destination), goAddress(msg.sender), goHash(msg.value).Big(),
		goByteSlice(msg.input_data, msg.input_size), int64(msg.gas), int(msg.depth), msg.flags != 0, goHash(msg.create2_salt).Big())

	statusCode := C.enum_evmc_status_code(0)
	if err != nil {
		statusCode = C.enum_evmc_status_code(err.(Error))
	}

	outputData := (*C.uint8_t)(nil)
	if len(output) > 0 {
		outputData = (*C.uint8_t)(&output[0])
	}

	result := C.evmc_make_result(statusCode, C.int64_t(gasLeft), outputData, C.size_t(len(output)))
	result.create_address = evmcAddress(createAddr)
	return result
}
