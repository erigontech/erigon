package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

type IssuanceCfg struct {
	db          kv.RwDB
	chainConfig *params.ChainConfig
}

func StageIssuanceCfg(db kv.RwDB, chainConfig *params.ChainConfig) IssuanceCfg {
	return IssuanceCfg{
		db:          db,
		chainConfig: chainConfig,
	}
}

func SpawnStageIssuance(cfg IssuanceCfg, s *StageState, tx kv.RwTx, ctx context.Context) error {
	useExternalTx := tx != nil

	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	headNumber, err := stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}

	if cfg.chainConfig.Consensus != params.EtHashConsensus {
		if err = s.Update(tx, headNumber); err != nil {
			return err
		}
		if !useExternalTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}
	// Log timer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	// Read previous issuance
	totalIssued, err := rawdb.ReadTotalIssued(tx, s.BlockNumber)
	if err != nil {
		return err
	}

	totalBurnt, err := rawdb.ReadTotalBurnt(tx, s.BlockNumber)
	if err != nil {
		return err
	}

	stopped := false
	prevProgress := s.BlockNumber
	currentBlockNumber := s.BlockNumber + 1
	for ; currentBlockNumber < headNumber && !stopped; currentBlockNumber++ {
		// read body without transactions
		hash, err := rawdb.ReadCanonicalHash(tx, currentBlockNumber)
		if err != nil {
			return err
		}
		body, _, _ := rawdb.ReadBody(tx, hash, currentBlockNumber)
		if body == nil {
			return fmt.Errorf("could not find block body for number: %d", currentBlockNumber)
		}
		header := rawdb.ReadHeader(tx, hash, currentBlockNumber)

		if header == nil {
			return fmt.Errorf("could not find block header for number: %d", currentBlockNumber)
		}

		burnt := big.NewInt(0)
		// burnt: len(Transactions) * baseFee * gasUsed
		if header.BaseFee != nil {
			burnt.Set(header.BaseFee)
			burnt.Mul(burnt, big.NewInt(int64(header.GasUsed)))
		}
		// TotalIssued, BlockReward and UncleReward, depends on consensus engine
		if header.Difficulty.Cmp(serenity.SerenityDifficulty) == 0 {
			// Proof-of-stake is 0.3 ether per block
			totalIssued.Add(totalIssued, serenity.RewardSerenity)
		} else {
			blockReward, uncleRewards := ethash.AccumulateRewards(cfg.chainConfig, header, body.Uncles)
			// Set BlockReward
			totalIssued.Add(totalIssued, blockReward.ToBig())
			// Compute uncleRewards
			for _, uncleReward := range uncleRewards {
				totalIssued.Add(totalIssued, uncleReward.ToBig())
			}
		}
		totalBurnt.Add(totalBurnt, burnt)
		// Write to database
		if err := rawdb.WriteTotalIssued(tx, currentBlockNumber, totalIssued); err != nil {
			return err
		}
		if err := rawdb.WriteTotalBurnt(tx, currentBlockNumber, totalBurnt); err != nil {
			return err
		}
		// Sleep and check for logs
		timer := time.NewTimer(1 * time.Nanosecond)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Wrote Block Issuance", s.LogPrefix()),
				"now", currentBlockNumber, "blk/sec", float64(currentBlockNumber-prevProgress)/float64(logInterval/time.Second))
			prevProgress = currentBlockNumber
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		}
		// Cleanup timer
		timer.Stop()
	}
	if err = s.Update(tx, currentBlockNumber); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindIssuanceStage(u *UnwindState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if err = u.Done(tx); err != nil {
		return fmt.Errorf(" reset: %w", err)
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to write db commit: %w", err)
		}
	}
	return nil
}

func PruneIssuanceStage(p *PruneState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
