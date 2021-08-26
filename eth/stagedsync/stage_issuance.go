package stagedsync

import (
	"context"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"math/big"
	"time"
)

type IssuanceCfg struct {
	db          kv.RwDB
	genesis     *core.Genesis
	chainConfig *params.ChainConfig
}

func StageIssuanceCfg(db kv.RwDB, genesis *core.Genesis, chainConfig *params.ChainConfig) IssuanceCfg {
	return IssuanceCfg{
		db,
		genesis,
		chainConfig,
	}
}

func SpawnIssuance(s *StageState, tx kv.RwTx, cfg IssuanceCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	logPrefix := s.LogPrefix()
	if endBlock > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Computing issuance", logPrefix), "from", s.BlockNumber, "to", endBlock)
	}

	startBlock := s.BlockNumber
	if startBlock > 0 {
		startBlock++
	}
	if err := computeIssuance(logPrefix, tx, cfg.genesis, cfg.chainConfig, startBlock, endBlock, ctx.Done()); err != nil {
		return err
	}

	if err := s.Update(tx, endBlock); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type Issuance struct {
	TotalIssued *uint256.Int
	TotalBurnt  *uint256.Int
}

func computeInitialAlloc(genesis *core.Genesis) *big.Int {
	total := big.NewInt(0)

	if genesis == nil {
		genesis = core.DefaultGenesisBlock()
	}
	for _, v := range genesis.Alloc {
		total.Add(total, v.Balance)
	}
	return total
}

func computeIssuance(logPrefix string, tx kv.RwTx, genesis *core.Genesis, chainConfig *params.ChainConfig, startBlock, endBlock uint64, quitCh <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	cursor, err := tx.RwCursor(kv.Issuance)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// Initialize from previous grandtotal
	totalIssued := uint256.NewInt(0)
	totalBurnt := uint256.NewInt(0)
	if startBlock == 0 {
		totalIssued.SetFromBig(computeInitialAlloc(genesis))
	} else {
		prevBlock := startBlock - 1

		key := dbutils.IssuanceKey(prevBlock)
		_, v, err := cursor.SeekExact(key)
		if err != nil {
			return err
		}

		if v != nil {
			lastTotals := new(Issuance)
			if err := rlp.DecodeBytes(v, lastTotals); err != nil {
				return err
			}

			totalIssued.Set(lastTotals.TotalIssued)
			totalBurnt.Set(lastTotals.TotalBurnt)
		}
	}

	for i := startBlock; i <= endBlock; i++ {
		select {
		default:
		case <-quitCh:
			return common.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "block", i)
		}

		blockHash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return err
		}
		header := rawdb.ReadHeader(tx, blockHash, i)
		body, _, _ := rawdb.ReadBody(tx, blockHash, i)

		// Calculate issuance for this block; accumulate it in grand total
		minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, header, body.Uncles)
		issued := minerReward
		for _, v := range uncleRewards {
			issued.Add(&issued, &v)
		}
		totalIssued.Add(totalIssued, &issued)

		// Calculate burnt for this block; accumulate it in grand total
		burnt := uint256.NewInt(0)
		if chainConfig.IsLondon(i) {
			gasUsed := uint256.NewInt(0)
			gasUsed.SetUint64(header.GasUsed)

			baseFee, overflow := uint256.FromBig(header.BaseFee)
			if overflow {
				log.Error("Overflow while reading basefee")
			} else {
				burnt.Mul(baseFee, gasUsed)
			}

			totalBurnt.Add(totalBurnt, burnt)
		}

		// Persist
		key := dbutils.IssuanceKey(i)
		value, err := rlp.EncodeToBytes(Issuance{totalIssued, totalBurnt})
		if err != nil {
			return err
		}
		if err := cursor.Append(key, value); err != nil {
			return err
		}
	}

	return nil
}

func UnwindIssuance(u *UnwindState, s *StageState, tx kv.RwTx, cfg IssuanceCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := u.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwinding issuance", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)
	cursor, err := tx.RwCursor(kv.Issuance)
	if err != nil {
		return err
	}
	defer cursor.Close()

	key := dbutils.IssuanceKey(u.UnwindPoint + 1)
	for k, _, err := cursor.SeekExact(key); k != nil; k, _, err = cursor.Next() {
		if err != nil {
			return err
		}

		if err = cursor.DeleteCurrent(); err != nil {
			return err
		}
	}

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneIssuance(p *PruneState, tx kv.RwTx, cfg IssuanceCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
