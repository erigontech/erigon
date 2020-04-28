package downloader

import (
	"context"
	"fmt"
	"math/big"

	"github.com/pkg/errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) spawnRecoverSendersStage() error {
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, Senders)
	if err != nil {
		return err
	}
	lastProcessedBlockNumber = 0

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

		jobs := make(chan *senderRecoveryJob, 0)
		out := make(chan *senderRecoveryJob, 0)
		cancel := make(chan struct{}, 0)

		fmt.Printf("before senders recovery")

		go recoverSenders(jobs, out, cancel)
		jobs <- &senderRecoveryJob{s, body, hash, nextBlockNumber, nil}

		<-out

		fmt.Printf("after senders recovery")

		close(cancel)
		close(jobs)
		close(out)

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

type senderRecoveryJob struct {
	signer          types.Signer
	blockBody       *types.Body
	hash            common.Hash
	nextBlockNumber uint64
	err             error
}

func recoverSenders(in chan *senderRecoveryJob, out chan *senderRecoveryJob, cancel chan struct{}) {
	for {
		select {
		case job := <-in:
			for _, tx := range job.blockBody.Transactions {
				from, err := types.Sender(job.signer, tx)
				if err != nil {
					job.err = errors.Wrap(err, fmt.Sprintf("error recovering sender for tx=%x\n", tx.Hash()))
					break
				}
				tx.SetFrom(from)
				if tx.Protected() && tx.ChainId().Cmp(job.signer.ChainId()) != 0 {
					job.err = errors.New("invalid chainId")
					break
				}
			}
			out <- job
		case <-cancel:
			return
		}
	}

	return
}
