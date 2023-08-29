package silkworm

/*
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// START silkworm_api.h: C API exported by Silkworm to be used in Erigon.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#ifndef SILKWORM_API_H_
#define SILKWORM_API_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#if defined _MSC_VER
#define SILKWORM_EXPORT __declspec(dllexport)
#else
#define SILKWORM_EXPORT __attribute__((visibility("default")))
#endif

#if __cplusplus
#define SILKWORM_NOEXCEPT noexcept
#else
#define SILKWORM_NOEXCEPT
#endif

#if __cplusplus
extern "C" {
#endif

typedef struct MDBX_txn MDBX_txn;

#define SILKWORM_OK                  0
#define SILKWORM_INTERNAL_ERROR      1
#define SILKWORM_UNKNOWN_ERROR       2
#define SILKWORM_INVALID_HANDLE      3
#define SILKWORM_INVALID_PATH        4
#define SILKWORM_INVALID_SNAPSHOT    5
#define SILKWORM_INVALID_MDBX_TXN    6
#define SILKWORM_INVALID_BLOCK_RANGE 7
#define SILKWORM_BLOCK_NOT_FOUND     8
#define SILKWORM_UNKNOWN_CHAIN_ID    9
#define SILKWORM_MDBX_ERROR          10
#define SILKWORM_INVALID_BLOCK       11
#define SILKWORM_DECODING_ERROR      12

typedef struct SilkwormHandle SilkwormHandle;

SILKWORM_EXPORT int silkworm_init(SilkwormHandle** handle) SILKWORM_NOEXCEPT;

struct SilkwormMemoryMappedFile {
    const char* file_path;
    uint8_t* memory_address;
    size_t memory_length;
};

struct SilkwormHeadersSnapshot {
    struct SilkwormMemoryMappedFile segment;
    struct SilkwormMemoryMappedFile header_hash_index;
};

struct SilkwormBodiesSnapshot {
    struct SilkwormMemoryMappedFile segment;
    struct SilkwormMemoryMappedFile block_num_index;
};

struct SilkwormTransactionsSnapshot {
    struct SilkwormMemoryMappedFile segment;
    struct SilkwormMemoryMappedFile tx_hash_index;
    struct SilkwormMemoryMappedFile tx_hash_2_block_index;
};

struct SilkwormChainSnapshot {
    struct SilkwormHeadersSnapshot headers;
    struct SilkwormBodiesSnapshot bodies;
    struct SilkwormTransactionsSnapshot transactions;
};

SILKWORM_EXPORT int silkworm_add_snapshot(SilkwormHandle* handle, struct SilkwormChainSnapshot* snapshot) SILKWORM_NOEXCEPT;

SILKWORM_EXPORT int silkworm_execute_blocks(
    SilkwormHandle* handle, MDBX_txn* txn, uint64_t chain_id, uint64_t start_block, uint64_t max_block,
    uint64_t batch_size, bool write_change_sets, bool write_receipts, bool write_call_traces,
    uint64_t* last_executed_block, int* mdbx_error_code) SILKWORM_NOEXCEPT;

SILKWORM_EXPORT int silkworm_fini(SilkwormHandle* handle) SILKWORM_NOEXCEPT;

#if __cplusplus
}
#endif

#endif  // SILKWORM_API_H_

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// END silkworm_api.h: C API exported by Silkworm to be used in Erigon.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

typedef int (*silkworm_init_func)(SilkwormHandle** handle);

int call_silkworm_init_func(void* func_ptr, SilkwormHandle** handle) {
    return ((silkworm_init_func)func_ptr)(handle);
}

typedef int (*silkworm_add_snapshot_func)(SilkwormHandle* handle, struct SilkwormChainSnapshot* snapshot);

int call_silkworm_add_snapshot_func(void* func_ptr, SilkwormHandle* handle, struct SilkwormChainSnapshot* snapshot) {
    return ((silkworm_add_snapshot_func)func_ptr)(handle, snapshot);
}

typedef int (*silkworm_execute_blocks_func)(SilkwormHandle* handle, MDBX_txn* txn, uint64_t chain_id, uint64_t start_block,
    uint64_t max_block, uint64_t batch_size, bool write_change_sets, bool write_receipts, bool write_call_traces,
	uint64_t* last_executed_block, int* mdbx_error_code);

int call_silkworm_execute_blocks_func(void* func_ptr, SilkwormHandle* handle, MDBX_txn* txn, uint64_t chain_id, uint64_t start_block,
	uint64_t max_block, uint64_t batch_size, bool write_change_sets, bool write_receipts, bool write_call_traces,
	uint64_t* last_executed_block, int* mdbx_error_code) {
    return ((silkworm_execute_blocks_func)func_ptr)(handle, txn, chain_id, start_block, max_block, batch_size, write_change_sets,
		write_receipts, write_call_traces, last_executed_block, mdbx_error_code);
}

typedef int (*silkworm_fini_func)(SilkwormHandle* handle);

int call_silkworm_fini_func(void* func_ptr, SilkwormHandle* handle) {
    return ((silkworm_fini_func)func_ptr)(handle);
}

*/
import "C"
import (
	"fmt"
	"math/big"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type Silkworm struct {
	dllHandle		unsafe.Pointer
	instance		*C.SilkwormHandle
	initFunc		unsafe.Pointer
	finiFunc		unsafe.Pointer
	addSnapshot		unsafe.Pointer
	executeBlocks	unsafe.Pointer
}

func New(dllPath string) (*Silkworm, error) {
	dllHandle, err := OpenLibrary(dllPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load silkworm library from path %s: %w", dllPath, err)
	}

	initFunc, err := LoadFunction(dllHandle, "silkworm_init")
	if err != nil {
		return nil, fmt.Errorf("failed to load silkworm function silkworm_init: %w", err)
	}
	finiFunc, err := LoadFunction(dllHandle, "silkworm_fini")
	if err != nil {
		return nil, fmt.Errorf("failed to load silkworm function silkworm_fini: %w", err)
	}
	addSnapshot, err := LoadFunction(dllHandle, "silkworm_add_snapshot")
	if err != nil {
		return nil, fmt.Errorf("failed to load silkworm function silkworm_add_snapshot: %w", err)
	}
	executeBlocks, err := LoadFunction(dllHandle, "silkworm_execute_blocks")
	if err != nil {
		return nil, fmt.Errorf("failed to load silkworm function silkworm_execute_blocks: %w", err)
	}

	silkworm := &Silkworm{
		dllHandle: dllHandle,
		initFunc: initFunc,
		finiFunc: finiFunc,
		addSnapshot: addSnapshot,
		executeBlocks: executeBlocks,
	}
	return silkworm, nil
}

func (s *Silkworm) Init() {
	C.call_silkworm_init_func(s.initFunc, &s.instance)
}

func (s *Silkworm) Fini() {
	C.call_silkworm_fini_func(s.finiFunc, s.instance)
}

func (s *Silkworm) AddSnapshot(snapshot *C.struct_SilkwormChainSnapshot) {
	C.call_silkworm_add_snapshot_func(s.addSnapshot, s.instance, snapshot)
}

func (s *Silkworm) ExecuteBlocks(txn kv.Tx, chainID *big.Int, startBlock uint64, maxBlock uint64, batchSize uint64, writeChangeSets, writeReceipts, writeCallTraces bool) (lastExecutedBlock uint64, err error) {
	cTxn := (*C.MDBX_txn)(txn.CHandle())
	cChainId := C.uint64_t(chainID.Uint64())
	cStartBlock := C.uint64_t(startBlock)
	cMaxBlock := C.uint64_t(maxBlock)
	cBatchSize := C.uint64_t(batchSize)
	cWriteChangeSets := C._Bool(writeChangeSets)
	cWriteReceipts := C._Bool(writeReceipts)
	cWriteCallTraces := C._Bool(writeCallTraces)
	cLastExecutedBlock := C.uint64_t(startBlock - 1)
	cMdbxErrorCode := C.int(0)
	status := C.call_silkworm_execute_blocks_func(s.executeBlocks, s.instance, cTxn, cChainId, cStartBlock,
		cMaxBlock, cBatchSize, cWriteChangeSets, cWriteReceipts, cWriteCallTraces, &cLastExecutedBlock, &cMdbxErrorCode)
	lastExecutedBlock = uint64(cLastExecutedBlock)
	if status == 0 || status == 8 {
		return lastExecutedBlock, nil
	}
	return lastExecutedBlock, fmt.Errorf("silkworm_execute_blocks error %d, MDBX error %d", status, cMdbxErrorCode)
}
