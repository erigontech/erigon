package stages

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

type StageBeaconIndexesCfg struct {
	db     kv.RwDB
	tmpdir string
}

func StageBeaconIndexes(db kv.RwDB, tmpdir string) StageBeaconIndexesCfg {
	return StageBeaconIndexesCfg{
		db:     db,
		tmpdir: tmpdir,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageBeaconIndexes(cfg StageBeaconIndexesCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	progress := s.BlockNumber
	if progress != 0 {
		progress++
	}

	rootToSlotCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer rootToSlotCollector.Close()

	endSlot, err := stages.GetStageProgress(tx, stages.BeaconBlocks)
	if err != nil {
		return err
	}
	logInterval := time.NewTicker(logIntervalTime)
	defer logInterval.Stop()

	for slot := progress; slot <= endSlot; slot++ {
		block, _, eth1Hash, eth2Hash, err := rawdb.ReadBeaconBlockForStorage(tx, slot)
		if err != nil {
			return err
		}
		// Missed proposal are absent slot
		if block == nil {
			continue
		}
		slotBytes := utils.Uint32ToBytes4(uint32(slot))

		if eth1Hash != (libcommon.Hash{}) {
			// Collect root indexes => slot
			if err := rootToSlotCollector.Collect(eth1Hash[:], slotBytes[:]); err != nil {
				return err
			}
		}

		if err := rootToSlotCollector.Collect(eth2Hash[:], slotBytes[:]); err != nil {
			return err
		}
		if err := rootToSlotCollector.Collect(block.Block.StateRoot[:], slotBytes[:]); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info(fmt.Sprintf("[%s] Progress", s.LogPrefix()), "slot", slot, "remaining", endSlot-slot)
		default:
		}
	}
	if err := rootToSlotCollector.Load(tx, kv.RootSlotIndex, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := s.Update(tx, endSlot); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
