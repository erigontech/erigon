package stagedsync

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type Stage3Config struct {
	BatchSize       int
	BlockSize       int
	BufferSize      int
	StartTrace      bool
	Prof            bool
	ToProcess       int
	NumOfGoroutines int
	ReadChLen       int
	Now             time.Time
}

func SpawnRecoverSendersStage(cfg Stage3Config, s *StageState, db ethdb.Database, config *params.ChainConfig, toBlock uint64, datadir string, quitCh <-chan struct{}) error {
	prevStageProgress, _, errStart := stages.GetStageProgress(db, stages.Bodies)
	if errStart != nil {
		return errStart
	}
	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to <= s.BlockNumber {
		s.Done()
		return nil
	}
	log.Info(fmt.Sprintf("[%s] Senders recovery", stages.Senders), "from", s.BlockNumber, "to", to)

	if cfg.StartTrace {
		filePath := fmt.Sprintf("trace_%d_%d_%d.out", cfg.Now.Day(), cfg.Now.Hour(), cfg.Now.Minute())
		f1, err := os.Create(filePath)
		if err != nil {
			return err
		}
		err = trace.Start(f1)
		if err != nil {
			return err
		}
		defer func() {
			trace.Stop()
			f1.Close()
		}()
	}
	if cfg.Prof {
		f2, err := os.Create(fmt.Sprintf("cpu_%d_%d_%d.prof", cfg.Now.Day(), cfg.Now.Hour(), cfg.Now.Minute()))
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		defer f2.Close()
		if err = pprof.StartCPUProfile(f2); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
	}

	canonical := make([]common.Hash, to-s.BlockNumber)
	currentHeaderIdx := uint64(0)

	if err := db.Walk(dbutils.HeaderPrefix, dbutils.EncodeBlockNumber(s.BlockNumber+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		// Skip non relevant records
		if !dbutils.CheckCanonicalKey(k) {
			return true, nil
		}

		if currentHeaderIdx >= to-s.BlockNumber { // if header stage is ehead of body stage
			return false, nil
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
		return true, nil
	}); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Reading canonical hashes complete", stages.Senders), "hashes", len(canonical))

	jobs := make(chan *senderRecoveryJob, cfg.BatchSize)
	go func() {
		defer close(jobs)

		if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(s.BlockNumber+1), 0, func(k, v []byte) (bool, error) {
			if err := common.Stopped(quitCh); err != nil {
				return false, err
			}

			blockNumber := binary.BigEndian.Uint64(k[:8])
			blockHash := common.BytesToHash(k[8:])
			if blockNumber > to {
				return false, nil
			}

			if canonical[blockNumber-s.BlockNumber-1] != blockHash {
				// non-canonical case
				return true, nil
			}

			data := make([]byte, len(v))
			copy(data, v)

			if cfg.Prof || cfg.StartTrace {
				if blockNumber == uint64(cfg.ToProcess) {
					// Flush the profiler
					pprof.StopCPUProfile()
					//common.SafeClose(quitCh)
					return false, nil
				}
			}

			bodyRlp, err := rawdb.DecompressBlockBody(data)
			if err != nil {
				return false, err
			}

			jobs <- &senderRecoveryJob{bodyRlp: bodyRlp, blockNumber: blockNumber, index: int(blockNumber - s.BlockNumber - 1)}

			return true, nil
		}); err != nil {
			log.Error("walking over the block bodies", "error", err)
		}
	}()

	out := make(chan *senderRecoveryJob, cfg.BatchSize)
	wg := new(sync.WaitGroup)
	wg.Add(cfg.NumOfGoroutines)
	for i := 0; i < cfg.NumOfGoroutines; i++ {
		go func(threadNo int) {
			defer wg.Done()
			// each goroutine gets it's own crypto context to make sure they are really parallel
			recoverSenders(secp256k1.ContextForThread(threadNo), config, jobs, out, quitCh)
		}(i)
	}

	log.Info(fmt.Sprintf("[%s] Started recoverer goroutines", stages.Senders), "numOfGoroutines", cfg.NumOfGoroutines)
	go func() {
		wg.Wait()
		close(out)
	}()

	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	for j := range out {
		if j.err != nil {
			return j.err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		k := make([]byte, 4)
		select {
		default:
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Senders recovery", stages.Senders), "number", j.index)
		}
		binary.BigEndian.PutUint32(k, uint32(j.index))
		if err := collector.Collect(k, j.senders); err != nil {
			return err
		}
	}
	loadFunc := func(k []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		index := int(binary.BigEndian.Uint32(k))
		return next(k, dbutils.BlockBodyKey(s.BlockNumber+uint64(index)+1, canonical[index]), value)
	}

	if err := collector.Load(db,
		dbutils.Senders,
		loadFunc,
		etl.TransformArgs{
			Quit: quitCh,
			LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"block", binary.BigEndian.Uint64(k)}
			},
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"block", binary.BigEndian.Uint64(k)}
			},
		},
	); err != nil {
		return err
	}

	return s.DoneAndUpdate(db, to)
}

type senderRecoveryJob struct {
	bodyRlp     rlp.RawValue
	blockNumber uint64
	index       int
	senders     []byte
	err         error
}

func recoverSenders(cryptoContext *secp256k1.Context, config *params.ChainConfig, in, out chan *senderRecoveryJob, quit <-chan struct{}) {
	for job := range in {
		if job == nil {
			return
		}
		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(job.bodyRlp), body); err != nil {
			job.err = fmt.Errorf("invalid block body RLP: %w", err)
			out <- job
			return
		}
		signer := types.MakeSigner(config, big.NewInt(int64(job.blockNumber)))
		job.senders = make([]byte, len(body.Transactions)*common.AddressLength)
		for i, tx := range body.Transactions {
			from, err := signer.SenderWithContext(cryptoContext, tx)
			if err != nil {
				job.err = fmt.Errorf("error recovering sender for tx=%x, %w", tx.Hash(), err)
				break
			}
			if tx.Protected() && tx.ChainID().Cmp(signer.ChainID()) != 0 {
				job.err = errors.New("invalid chainId")
				break
			}
			copy(job.senders[i*common.AddressLength:], from[:])
		}

		// prevent sending to close channel
		if err := common.Stopped(quit); err != nil {
			job.err = err
		}
		out <- job

		if errors.Is(job.err, common.ErrStopped) {
			return
		}
	}
}

func UnwindSendersStage(u *UnwindState, stateDB ethdb.Database) error {
	// Does not require any special processing
	mutation := stateDB.NewBatch()
	err := u.Done(mutation)
	if err != nil {
		return fmt.Errorf("unwind Senders: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind Senders: failed to write db commit: %v", err)
	}
	return nil
}
