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
	legacyVerifier := batchContext.cfg.legacyVerifier
	request := verifierBundle.Request
	blockNumber := request.GetLastBlockNumber()
	blockNumbers := []uint64{blockNumber}

	log.Info(fmt.Sprintf("[%s] identified an invalid batch, entering limbo", batchContext.s.LogPrefix()), "batch", request.BatchNumber)

	l1InfoTreeMinTimestamps := make(map[uint64]uint64)
	if _, err := legacyVerifier.GetWholeBatchStreamBytes(request.BatchNumber, batchContext.sdb.tx, blockNumbers, batchContext.sdb.hermezDb.HermezDbReader, l1InfoTreeMinTimestamps, nil); err != nil {
		return err
	}

	witness, err := legacyVerifier.WitnessGenerator.GetWitnessByBlockRange(batchContext.sdb.tx, batchContext.ctx, blockNumber, blockNumber, false, batchContext.cfg.zk.WitnessFull)
	if err != nil {
		return err
	}

	limboBlock := txpool.NewLimboBlockDetails()
	limboBlock.Witness = witness
	limboBlock.L1InfoTreeMinTimestamps = l1InfoTreeMinTimestamps
	limboBlock.BlockNumber = blockNumber
	limboBlock.BatchNumber = request.BatchNumber
	limboBlock.ForkId = request.ForkId

	block, err := rawdb.ReadBlockByNumber(batchContext.sdb.tx, blockNumber)
	if err != nil {
		return err
	}

	var transactionsToIncludeByIndex [][]int = [][]int{
		make([]int, 0, len(block.Transactions())),
	}
	for i, transaction := range block.Transactions() {
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

		transactionsToIncludeByIndex[0] = append(transactionsToIncludeByIndex[0], i)
		streamBytes, err := legacyVerifier.GetWholeBatchStreamBytes(request.BatchNumber, batchContext.sdb.tx, blockNumbers, batchContext.sdb.hermezDb.HermezDbReader, l1InfoTreeMinTimestamps, transactionsToIncludeByIndex)
		if err != nil {
			return err
		}

		hash := transaction.Hash()
		limboBlock.AppendTransaction(buffer.Bytes(), streamBytes, hash, sender)

		log.Info(fmt.Sprintf("[%s] adding transaction to limbo", batchContext.s.LogPrefix()), "hash", hash)
	}

	limboBlock.BlockTimestamp = block.Time()
	batchContext.cfg.txPool.ProcessLimboBlockDetails(limboBlock)
	return nil
}
