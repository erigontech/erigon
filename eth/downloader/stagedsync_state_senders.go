package downloader

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"math/big"
)

func (d *Downloader) spawnRecoverSendersStage() error {
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, Senders)
	if err != nil {
		return err
	}

	nextBlockNumber := lastProcessedBlockNumber + 1

	mutation := d.stateDB.NewBatch()
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Senders): failed to write db commit", "err", dbErr)
		}
	}()

	config := d.blockchain.Config()
	emptyHash := common.Hash{}
	var blockNumber big.Int

	for {
		hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
		if hash == emptyHash {
			break
		}
		body := rawdb.ReadBody(mutation, hash, nextBlockNumber)
		if body == nil {
			break
		}
		blockNumber.SetUint64(nextBlockNumber)
		s := types.MakeSigner(config, &blockNumber)
		for _, tx := range body.Transactions {
			from, err1 := types.Sender(s, tx)
			if err1 != nil {
				log.Error("Recovering sender from signature", "tx", tx.Hash(), "block", nextBlockNumber, "error", err1)
				break
			}
			tx.SetFrom(from)
			if tx.Protected() && tx.ChainId().Cmp(s.ChainId()) != 0 {
				log.Error("Invalid chainId", "tx", tx.Hash(), "block", nextBlockNumber, "tx.chainId", tx.ChainId(), "expected", s.ChainId())
				break
			}
		}

		rawdb.WriteBody(context.Background(), mutation, hash, nextBlockNumber, body)

		if nextBlockNumber%1000 == 0 {
			log.Info("Recovered for blocks:", "blockNumber", nextBlockNumber)
		}

		if err = SaveStageProgress(mutation, Senders, nextBlockNumber); err != nil {
			return err
		}

		nextBlockNumber++

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if _, err = mutation.Commit(); err != nil {
				return err
			}
			mutation = d.stateDB.NewBatch()
		}
	}

	return nil
}
