package silkworm

/*
#include <stdbool.h>
#include <stdint.h>

typedef int (*SilkwormExecuteBlocksFunctionPointer)(void* txn, uint64_t chain_id, uint64_t start_block,
                                                    uint64_t max_block, uint64_t batch_size, bool write_receipts,
                                                    uint64_t* last_executed_block, int* lmdb_error_code);

int call_silkworm_execute_blocks(void* func_ptr, void* txn, uint64_t chain_id, uint64_t start_block, uint64_t max_block,
                                 uint64_t batch_size, bool write_receipts, uint64_t* last_executed_block,
                                 int* lmdb_error_code) {
    return ((SilkwormExecuteBlocksFunctionPointer)func_ptr)(txn, chain_id, start_block, max_block, batch_size,
                                                            write_receipts, last_executed_block, lmdb_error_code);
}
*/
import "C"

import (
	"fmt"
	"math/big"
	"unsafe"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func ExecuteBlocks(funcPtr unsafe.Pointer, txn ethdb.Tx, chainID *big.Int, startBlock uint64, maxBlock uint64, batchSize int, writeReceipts bool) (executedBlock uint64, err error) {
	cChainId := C.uint64_t(chainID.Uint64())
	cStartBlock := C.uint64_t(startBlock)
	cMaxBlock := C.uint64_t(maxBlock)
	cBatchSize := C.uint64_t(batchSize)
	cWriteReceipts := C._Bool(writeReceipts)
	cLastExecutedBlock := C.uint64_t(startBlock - 1)
	cLmdbErrorCode := C.int(0)
	status := C.call_silkworm_execute_blocks(funcPtr, txn.CHandle(), cChainId, cStartBlock, cMaxBlock, cBatchSize, cWriteReceipts, &cLastExecutedBlock, &cLmdbErrorCode)
	executedBlock = uint64(cLastExecutedBlock)
	if status == 0 || status == 1 {
		return executedBlock, nil
	}
	return executedBlock, fmt.Errorf("silkworm_execute_blocks error %d, LMDB error %d", status, cLmdbErrorCode)
}
