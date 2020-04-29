package downloader

import (
	"context"
	"fmt"
	"math/big"
	"runtime"

	"github.com/pkg/errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/log"
)

var numOfGoroutines int
var cryptoContexts []*secp256k1.Context

func init() {
	// To avoid bothering with creating/releasing the resources
	// but still not leak the contexts
	numOfGoroutines = runtime.NumCPU()
	cryptoContexts = make([]*secp256k1.Context, numOfGoroutines)
	for i := 0; i < numOfGoroutines; i++ {
		cryptoContexts[i] = secp256k1.NewContext()
	}
}

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

	const batchSize = 10000

	jobs := make(chan *senderRecoveryJob, batchSize)
	out := make(chan *senderRecoveryJob, batchSize)

	defer func() {
		close(jobs)
		close(out)
	}()

	for i := 0; i < numOfGoroutines; i++ {
		// each goroutine gets it's own crypto context to make sure they are really parallel
		go recoverSenders(cryptoContexts[i], jobs, out)
	}
	log.Info("Sync (Senders): Started recoverer goroutines", "numOfGoroutines", numOfGoroutines)

	needExit := false
	for !needExit {
		written := 0
		for i := 0; i < batchSize; i++ {
			hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
			if hash == emptyHash {
				needExit = true
				break
			}
			body := rawdb.ReadBody(mutation, hash, nextBlockNumber)
			if body == nil {
				needExit = true
				break
			}
			blockNumber.SetUint64(nextBlockNumber)
			s := types.MakeSigner(config, &blockNumber)

			jobs <- &senderRecoveryJob{s, body, hash, nextBlockNumber, nil}
			written++

			nextBlockNumber++
		}

		for i := 0; i < written; i++ {
			j := <-out
			if j.err != nil {
				return errors.Wrap(j.err, "could not extract senders")
			}
			rawdb.WriteBody(context.Background(), mutation, j.hash, j.nextBlockNumber, j.blockBody)
		}

		if err = SaveStageProgress(mutation, Senders, nextBlockNumber); err != nil {
			return err
		}

		log.Info("Recovered for blocks:", "blockNumber", nextBlockNumber)

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

func recoverSenders(cryptoContext *secp256k1.Context, in chan *senderRecoveryJob, out chan *senderRecoveryJob) {
	var job *senderRecoveryJob
	for {
		job = <-in
		if job == nil {
			return
		}
		for _, tx := range job.blockBody.Transactions {
			from, err := job.signer.SenderWithContext(cryptoContext, tx)
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
	}
}
