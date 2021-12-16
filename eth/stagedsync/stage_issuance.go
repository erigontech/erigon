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
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

type IssuanceCfg struct {
	db          kv.RwDB
	chainConfig *params.ChainConfig
	issuance    bool
}

func StageIssuanceCfg(db kv.RwDB, chainConfig *params.ChainConfig, issuance bool) IssuanceCfg {
	return IssuanceCfg{
		db:          db,
		chainConfig: chainConfig,
		issuance:    issuance,
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

	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}

	if !cfg.issuance || cfg.chainConfig.Consensus != params.EtHashConsensus {
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
	netIssuance := big.NewInt(0)
	blockIssuance, err := rawdb.ReadIssuance(tx, s.BlockNumber)
	if err != nil {
		return err
	}
	netIssuance.Set(blockIssuance.Issuance)

	stopped := false
	prevProgress := s.BlockNumber
	for currentBlockNumber := s.BlockNumber + 1; currentBlockNumber < headNumber && !stopped; currentBlockNumber++ {
		// read body + transactions
		hash, err := rawdb.ReadCanonicalHash(tx, currentBlockNumber)
		if err != nil {
			return err
		}
		body := rawdb.ReadBodyWithTransactions(tx, hash, currentBlockNumber)
		if body == nil {
			return fmt.Errorf("could not find block body for number: %d", currentBlockNumber)
		}

		header := rawdb.ReadHeader(tx, hash, currentBlockNumber)

		if header == nil {
			return fmt.Errorf("could not find block header for number: %d", currentBlockNumber)
		}
		// Computations
		issuance := types.NewBlockIssuance()

		// TotalBurnt: len(Transactions) * baseFee
		if header.BaseFee != nil {
			issuance.TotalBurnt.Set(header.BaseFee)
			issuance.TotalBurnt.Mul(issuance.TotalBurnt, big.NewInt(int64(len(body.Transactions))))
		}

		// TotalIssued, BlockReward and UncleReward, depends on consensus engine
		if header.Difficulty.Cmp(serenity.SerenityDifficulty) == 0 {
			// Proof-of-stake is 0.3 ether per block
			issuance.TotalIssued.Set(serenity.RewardSerenity)
			issuance.BlockReward.Set(serenity.RewardSerenity)
		} else {
			blockReward, uncleRewards := ethash.AccumulateRewards(cfg.chainConfig, header, body.Uncles)
			// Set BlockReward
			issuance.BlockReward.Set(blockReward.ToBig())
			// Compute uncleRewards
			issuance.TotalIssued.Set(issuance.BlockReward)
			for _, uncleReward := range uncleRewards {
				issuance.UncleReward.Add(issuance.UncleReward, uncleReward.ToBig())
			}
			// Compute totalIssued: uncleReward + blockReward
			issuance.TotalIssued.Add(issuance.BlockReward, issuance.UncleReward)
		}
		// Compute issuance
		issuance.Issuance.Set(netIssuance)
		issuance.Issuance.Add(issuance.Issuance, issuance.TotalIssued)
		issuance.Issuance.Sub(issuance.Issuance, issuance.TotalBurnt)
		// Update net issuance
		netIssuance.Set(issuance.Issuance)
		// Write to database
		if err := rawdb.WriteIssuance(tx, currentBlockNumber, issuance); err != nil {
			return err
		}
		// Sleep and check for logs
		timer := time.NewTimer(1 * time.Microsecond)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info("Wrote Block Issuance",
				"now", currentBlockNumber, "blk/sec", float64(currentBlockNumber-prevProgress)/float64(logInterval/time.Second))
			prevProgress = currentBlockNumber
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		}
		// Cleanup timer
		timer.Stop()
	}
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
