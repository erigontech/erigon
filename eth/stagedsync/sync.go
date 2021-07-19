package stagedsync

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
)

type Sync struct {
	unwindPoint     *uint64 // used to run stages
	prevUnwindPoint *uint64 // used to get value from outside of staged sync after cycle (for example to notify RPCDaemon)
	badBlock        common.Hash

	stages       []*Stage
	unwindOrder  []*Stage
	pruningOrder []*Stage
	currentStage uint
}

func (s *Sync) Len() int                 { return len(s.stages) }
func (s *Sync) PrevUnwindPoint() *uint64 { return s.prevUnwindPoint }

func (s *Sync) NewUnwindState(id stages.SyncStage, unwindPoint, currentProgress uint64) *UnwindState {
	return &UnwindState{id, unwindPoint, currentProgress, common.Hash{}, s}
}

func (s *Sync) NewPruneState(id stages.SyncStage, prunePoint, currentProgress uint64) *PruneState {
	return &PruneState{id, prunePoint, currentProgress, s}
}

func (s *Sync) NextStage() {
	if s == nil {
		return
	}
	s.currentStage++
}

// IsBefore returns true if stage1 goes before stage2 in staged sync
func (s *Sync) IsBefore(stage1, stage2 stages.SyncStage) bool {
	idx1 := -1
	idx2 := -1
	for i, stage := range s.stages {
		if stage.ID == stage1 {
			idx1 = i
		}

		if stage.ID == stage2 {
			idx2 = i
		}
	}

	return idx1 < idx2
}

// IsAfter returns true if stage1 goes after stage2 in staged sync
func (s *Sync) IsAfter(stage1, stage2 stages.SyncStage) bool {
	idx1 := -1
	idx2 := -1
	for i, stage := range s.stages {
		if stage.ID == stage1 {
			idx1 = i
		}

		if stage.ID == stage2 {
			idx2 = i
		}
	}

	return idx1 > idx2
}

func (s *Sync) UnwindTo(unwindPoint uint64, badBlock common.Hash) {
	log.Info("UnwindTo", "block", unwindPoint, "bad_block_hash", badBlock.String())
	s.unwindPoint = &unwindPoint
	s.badBlock = badBlock
}

func (s *Sync) IsDone() bool {
	return s.currentStage >= uint(len(s.stages)) && s.unwindPoint == nil
}

func (s *Sync) LogPrefix() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%d/%d %s", s.currentStage+1, s.Len(), s.stages[s.currentStage].ID)
}

func (s *Sync) SetCurrentStage(id stages.SyncStage) error {
	for i, stage := range s.stages {
		if stage.ID == id {
			s.currentStage = uint(i)
			return nil
		}
	}
	return fmt.Errorf("stage not found with id: %v", id)
}

func New(stagesList []*Stage, unwindOrder UnwindOrder, pruneOrder PruneOrder) *Sync {
	unwindStages := make([]*Stage, len(stagesList))
	for i, stageIndex := range unwindOrder {
		for _, s := range stagesList {
			if s.ID == stageIndex {
				unwindStages[i] = s
				break
			}
		}
	}
	pruneStages := make([]*Stage, len(stagesList))
	for i, stageIndex := range pruneOrder {
		for _, s := range stagesList {
			if s.ID == stageIndex {
				pruneStages[i] = s
				break
			}
		}
	}

	return &Sync{
		stages:       stagesList,
		currentStage: 0,
		unwindOrder:  unwindStages,
		//pruningOrder: pruneStages,
	}
}

func (s *Sync) StageState(stage stages.SyncStage, tx ethdb.Tx, db ethdb.RoKV) (*StageState, error) {
	var blockNum uint64
	var err error
	useExternalTx := tx != nil
	if useExternalTx {
		blockNum, err = stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
	} else {
		if err = db.View(context.Background(), func(tx ethdb.Tx) error {
			blockNum, err = stages.GetStageProgress(tx, stage)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	return &StageState{s, stage, blockNum}, nil
}

func (s *Sync) Run(db ethdb.RwKV, tx ethdb.RwTx, firstCycle bool) error {
	s.prevUnwindPoint = nil
	var timings []interface{}
	for !s.IsDone() {
		if s.unwindPoint != nil {
			for j := 0; j < len(s.unwindOrder); j++ {
				if s.unwindOrder[j].Disabled || s.unwindOrder[j].Unwind == nil {
					continue
				}
				t := time.Now()
				if err := s.unwindStage(firstCycle, s.unwindOrder[j], db, tx); err != nil {
					return err
				}
				timings = append(timings, "Unwind "+string(s.unwindOrder[j].ID), time.Since(t))
			}
			s.prevUnwindPoint = s.unwindPoint
			s.unwindPoint = nil
			s.badBlock = common.Hash{}
			if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
				return err
			}
		}

		stage := s.stages[s.currentStage]

		if string(stage.ID) == debug.StopBeforeStage() { // stop process for debugging reasons
			log.Error("STOP_BEFORE_STAGE env flag forced to stop app")
			os.Exit(1)
		}

		if stage.Disabled || stage.Forward == nil {
			logPrefix := s.LogPrefix()
			log.Debug(fmt.Sprintf("[%s] disabled. %s", logPrefix, stage.DisabledDescription))

			s.NextStage()
			continue
		}

		t := time.Now()
		if err := s.runStage(stage, db, tx, firstCycle); err != nil {
			return err
		}
		timings = append(timings, string(stage.ID), time.Since(t))

		s.NextStage()
	}

	for i := 0; i < len(s.pruningOrder); i++ {
		if s.pruningOrder[i].Disabled || s.pruningOrder[i].Prune == nil {
			continue
		}
		t := time.Now()
		if err := s.pruneStage(firstCycle, s.pruningOrder[i], db, tx); err != nil {
			return err
		}
		timings = append(timings, "Pruning "+string(s.pruningOrder[i].ID), time.Since(t))
	}
	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return err
	}

	if err := printLogs(tx, timings); err != nil {
		return err
	}
	s.currentStage = 0
	return nil
}

func printLogs(tx ethdb.RwTx, timings []interface{}) error {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory", "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	if len(timings) > 50 {
		log.Info("Timings (first 50)", timings[:50]...)
	} else {
		log.Info("Timings", timings...)
	}

	if tx == nil {
		return nil
	}
	buckets := []string{
		"freelist",
		dbutils.PlainStateBucket,
		dbutils.AccountChangeSetBucket,
		dbutils.StorageChangeSetBucket,
		dbutils.EthTx,
		dbutils.Log,
	}
	bucketSizes := make([]interface{}, 0, 2*len(buckets))
	for _, bucket := range buckets {
		sz, err1 := tx.BucketSize(bucket)
		if err1 != nil {
			return err1
		}
		bucketSizes = append(bucketSizes, bucket, common.StorageSize(sz))
	}
	log.Info("Tables", bucketSizes...)
	tx.CollectMetrics()
	return nil
}

func (s *Sync) runStage(stage *Stage, db ethdb.RwKV, tx ethdb.RwTx, firstCycle bool) (err error) {
	stageState, err := s.StageState(stage.ID, tx, db)
	if err != nil {
		return err
	}

	start := time.Now()
	logPrefix := s.LogPrefix()
	if err = stage.Forward(firstCycle, stageState, s, tx); err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info(fmt.Sprintf("[%s] DONE", logPrefix), "in", time.Since(start))
	}
	return nil
}

func (s *Sync) unwindStage(firstCycle bool, stage *Stage, db ethdb.RwKV, tx ethdb.RwTx) error {
	start := time.Now()
	log.Info("Unwind...", "stage", stage.ID)
	stageState, err := s.StageState(stage.ID, tx, db)
	if err != nil {
		return err
	}

	unwind := s.NewUnwindState(stage.ID, *s.unwindPoint, stageState.BlockNumber)
	unwind.BadBlock = s.badBlock

	if stageState.BlockNumber <= unwind.UnwindPoint {
		return nil
	}

	if err = s.SetCurrentStage(stage.ID); err != nil {
		return err
	}

	err = stage.Unwind(firstCycle, unwind, stageState, tx)
	if err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info("Unwind... DONE!", "stage", string(unwind.ID))
	}
	return nil
}

func (s *Sync) pruneStage(firstCycle bool, stage *Stage, db ethdb.RwKV, tx ethdb.RwTx) error {
	start := time.Now()
	log.Info("Prune...", "stage", stage.ID)

	stageState, err := s.StageState(stage.ID, tx, db)
	if err != nil {
		return err
	}

	prunePoint := stageState.BlockNumber - params.FullImmutabilityThreshold // TODO: cli-customizable
	prune := s.NewPruneState(stage.ID, prunePoint, stageState.BlockNumber)
	if stageState.BlockNumber <= prune.PrunePoint {
		return nil
	}
	if err = s.SetCurrentStage(stage.ID); err != nil {
		return err
	}

	err = stage.Prune(firstCycle, prune, tx)
	if err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info("Prune... DONE!", "stage", string(prune.ID))
	}
	return nil
}

func (s *Sync) DisableAllStages() {
	for i := range s.stages {
		s.stages[i].Disabled = true
	}
}

func (s *Sync) DisableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = true
		}
	}
}

func (s *Sync) EnableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = false
		}
	}
}

func (s *Sync) MockExecFunc(id stages.SyncStage, f ExecFunc) {
	for i := range s.stages {
		if s.stages[i].ID == id {
			s.stages[i].Forward = f
		}
	}
}
