package stages

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
)

func handleLimbo(batchContext *BatchContext, batchState *BatchState, verifierBundle *legacy_executor_verifier.VerifierBundle) error {
	request := verifierBundle.Request
	legacyVerifier := batchContext.cfg.legacyVerifier

	log.Info(fmt.Sprintf("[%s] identified an invalid batch, entering limbo", batchContext.s.LogPrefix()), "batch", request.BatchNumber)

	l1InfoTreeMinTimestamps := make(map[uint64]uint64)
	if _, err := legacyVerifier.GetWholeBatchStreamBytes(request.BatchNumber, batchContext.sdb.tx, request.BlockNumbers, batchContext.sdb.hermezDb.HermezDbReader, l1InfoTreeMinTimestamps, nil); err != nil {
		return err
	}

	witness, err := legacyVerifier.WitnessGenerator.GetWitnessByBlockRange(batchContext.sdb.tx, batchContext.ctx, request.GetFirstBlockNumber(), request.GetLastBlockNumber(), false, batchContext.cfg.zk.WitnessFull)
	if err != nil {
		return err
	}

	limboDetails := txpool.NewLimboBatchDetails()
	limboDetails.Witness = witness
	limboDetails.L1InfoTreeMinTimestamps = l1InfoTreeMinTimestamps
	limboDetails.BatchNumber = request.BatchNumber
	limboDetails.ForkId = request.ForkId

	var blocksForStreamBytes []uint64 = make([]uint64, 0, len(request.BlockNumbers))
	var transactionsToIncludeByIndex [][]int = make([][]int, 0, len(request.BlockNumbers))
	for i, blockNumber := range request.BlockNumbers {
		block, err := rawdb.ReadBlockByNumber(batchContext.sdb.tx, blockNumber)
		if err != nil {
			return err
		}

		blocksForStreamBytes = append(blocksForStreamBytes, blockNumber)
		transactionsToIncludeByIndex = append(transactionsToIncludeByIndex, make([]int, 0, len(block.Transactions())))

		limboBlock := limboDetails.AppendBlock(blockNumber, block.Time())

		for j, transaction := range block.Transactions() {
			var b []byte
			buffer := bytes.NewBuffer(b)
			err = transaction.EncodeRLP(buffer)
			if err != nil {
				return err
			}

			signer := types.MakeSigner(batchContext.cfg.chainConfig, blockNumber)
			sender, err := transaction.Sender(*signer)
			if err != nil {
				return err
			}

			transactionsToIncludeByIndex[i] = append(transactionsToIncludeByIndex[i], j)
			streamBytes, err := legacyVerifier.GetWholeBatchStreamBytes(request.BatchNumber, batchContext.sdb.tx, blocksForStreamBytes, batchContext.sdb.hermezDb.HermezDbReader, l1InfoTreeMinTimestamps, transactionsToIncludeByIndex)
			if err != nil {
				return err
			}

			hash := transaction.Hash()
			limboBlock.AppendTransaction(buffer.Bytes(), streamBytes, hash, sender)

			log.Info(fmt.Sprintf("[%s] adding transaction to limbo", batchContext.s.LogPrefix()), "hash", hash)
		}
	}

	batchContext.cfg.txPool.ProcessLimboBatchDetails(limboDetails)
	return nil
}
