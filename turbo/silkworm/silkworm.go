package silkworm

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

typedef int (*SilkwormExecuteBlocksFunctionPointer)(void* txn, uint64_t chain_id, uint64_t start_block,
                                                    uint64_t batch_size, bool write_receipts,
                                                    uint64_t* last_executed_block, int* lmdb_error_code);

int call_silkworm_execute_blocks(void* func_ptr, void* txn, uint64_t chain_id, uint64_t start_block, uint64_t batch_size,
                                 bool write_receipts, uint64_t* last_executed_block, int* lmdb_error_code) {
    return ((SilkwormExecuteBlocksFunctionPointer)func_ptr)(txn, chain_id, start_block, batch_size, write_receipts,
                                                            last_executed_block, lmdb_error_code);
}
*/
import "C"

import (
	"fmt"
	"math/big"
	"unsafe"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const funcName = "silkworm_execute_blocks"

func LoadExecutionFunctionPointer(dllPath string) (unsafe.Pointer, error) {
	cPath := C.CString(dllPath)
	defer C.free(unsafe.Pointer(cPath))
	dllHandle := C.dlopen(cPath, C.RTLD_LAZY)
	if dllHandle == nil {
		err := C.GoString(C.dlerror())
		return nil, fmt.Errorf("failed to load dynamic library %s: %s", dllPath, err)
	}

	cName := C.CString(funcName)
	defer C.free(unsafe.Pointer(cName))
	funcPtr := C.dlsym(dllHandle, cName)
	if funcPtr == nil {
		err := C.GoString(C.dlerror())
		return nil, fmt.Errorf("failed to find the %s function: %s", funcName, err)
	}

	return funcPtr, nil
}

func ExecuteBlocks(funcPtr unsafe.Pointer, txn ethdb.Tx, chainID *big.Int, startBlock uint64, batchSize uint64, writeReceipts bool) (executedBlock uint64, err error) {
	cChainId := C.uint64_t(chainID.Uint64())
	cStartBlock := C.uint64_t(startBlock)
	cBatchSize := C.uint64_t(batchSize)
	cWriteReceipts := C._Bool(writeReceipts)
	cLastExecutedBlock := C.uint64_t(startBlock - 1)
	cLmdbErrorCode := C.int(0)
	status := C.call_silkworm_execute_blocks(funcPtr, txn.CHandle(), cChainId, cStartBlock, cBatchSize, cWriteReceipts, &cLastExecutedBlock, &cLmdbErrorCode)
	executedBlock = uint64(cLastExecutedBlock)
	if status == 0 || status == 1 {
		return executedBlock, nil
	}
	return executedBlock, fmt.Errorf("silkworm_execute_blocks error %d, LMDB error %d", status, cLmdbErrorCode)
}
