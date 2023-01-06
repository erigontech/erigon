package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type IssuanceCfg struct {
	db              kv.RwDB
	chainConfig     *params.ChainConfig
	blockReader     services.FullBlockReader
	enabledIssuance bool
}

func StageIssuanceCfg(db kv.RwDB, chainConfig *params.ChainConfig, blockReader services.FullBlockReader, enabledIssuance bool) IssuanceCfg {
	return IssuanceCfg{
		db:              db,
		chainConfig:     chainConfig,
		blockReader:     blockReader,
		enabledIssuance: enabledIssuance,
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

	if cfg.chainConfig.Consensus != params.EtHashConsensus || !cfg.enabledIssuance || headNumber == s.BlockNumber {
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
	headerC, err := tx.Cursor(kv.Headers)
	if err != nil {
		return err
	}
	for k, v, err := headerC.Seek(common2.EncodeTs(currentBlockNumber)); k != nil && !stopped; k, v, err = headerC.Next() {
		if err != nil {
			return err
		}

		if len(k) != 40 {
			continue
		}

		currentBlockNumber, err = dbutils.DecodeBlockNumber(k[:8])
		if err != nil {
			return err
		}
		if currentBlockNumber > headNumber {
			currentBlockNumber = headNumber
			break
		}
		// read body without transactions
		hash, err := rawdb.ReadCanonicalHash(tx, currentBlockNumber)
		if err != nil {
			return err
		}

		if hash != common.BytesToHash(k[8:]) {
			continue
		}
		var header types.Header
		if err := rlp.Decode(bytes.NewReader(v), &header); err != nil {
			log.Error("Invalid block header RLP", "hash", hash, "err", err)
			return nil
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
			var blockReward uint256.Int
			var uncleRewards []uint256.Int
			if header.UncleHash == types.EmptyUncleHash {
				blockReward, uncleRewards = ethash.AccumulateRewards(cfg.chainConfig, &header, nil)
			} else {
				body, _, err := cfg.blockReader.Body(ctx, tx, hash, currentBlockNumber)
				if err != nil {
					return err
				}
				blockReward, uncleRewards = ethash.AccumulateRewards(cfg.chainConfig, &header, body.Uncles)
			}
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
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Wrote Block Issuance", s.LogPrefix()),
				"now", currentBlockNumber, "blk/sec", float64(currentBlockNumber-prevProgress)/float64(logInterval/time.Second))
			prevProgress = currentBlockNumber
		default:
			log.Trace("RequestQueueTime (header) ticked")
		}
		// Cleanup timer
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

func UnwindIssuanceStage(u *UnwindState, cfg IssuanceCfg, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

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

func PruneIssuanceStage(p *PruneState, cfg IssuanceCfg, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
