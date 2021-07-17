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
)

type StagedSync struct {
	unwindPoint     *uint64 // used to run stages
	prevUnwindPoint *uint64 // used to get value from outside of staged sync after cycle (for example to notify RPCDaemon)
	badBlock        common.Hash

	stages       []*Stage
	unwindOrder  []*Stage
	currentStage uint
}

func (s *StagedSync) Len() int                 { return len(s.stages) }
func (s *StagedSync) PrevUnwindPoint() *uint64 { return s.prevUnwindPoint }

func (s *StagedSync) NewUnwindState(id stages.SyncStage, unwindPoint, currentProgress uint64) *UnwindState {
	return &UnwindState{id, unwindPoint, currentProgress, common.Hash{}, s}
}

func (s *StagedSync) NextStage() {
	if s == nil {
		return
	}
	s.currentStage++
}

// IsBefore returns true if stage1 goes before stage2 in staged sync
func (s *StagedSync) IsBefore(stage1, stage2 stages.SyncStage) bool {
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
func (s *StagedSync) IsAfter(stage1, stage2 stages.SyncStage) bool {
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

func (s *StagedSync) GetLocalHeight(db ethdb.KVGetter) (uint64, error) {
	state, err := s.StageState(stages.Headers, db)
	return state.BlockNumber, err
}

func (s *StagedSync) UnwindTo(unwindPoint uint64, badBlock common.Hash) {
	log.Info("UnwindTo", "block", unwindPoint, "bad_block_hash", badBlock.String())
	s.unwindPoint = &unwindPoint
	s.badBlock = badBlock
}

func (s *StagedSync) IsDone() bool {
	return s.currentStage >= uint(len(s.stages)) && s.unwindPoint == nil
}

func (s *StagedSync) CurrentStage() (uint, *Stage) {
	return s.currentStage, s.stages[s.currentStage]
}

func (s *StagedSync) LogPrefix() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%d/%d %s", s.currentStage+1, s.Len(), s.stages[s.currentStage].ID)
}

func (s *StagedSync) SetCurrentStage(id stages.SyncStage) error {
	for i, stage := range s.stages {
		if stage.ID == id {
			s.currentStage = uint(i)
			return nil
		}
	}
	return fmt.Errorf("stage not found with id: %v", id)
}

func (s *StagedSync) StageByID(id stages.SyncStage) (*Stage, error) {
	for _, stage := range s.stages {
		if stage.ID == id {
			return stage, nil
		}
	}
	return nil, fmt.Errorf("stage not found with id: %v", id)
}

func New(stagesList []*Stage, unwindOrder []stages.SyncStage) *StagedSync {
	unwindStages := make([]*Stage, len(stagesList))

	for i, stageIndex := range unwindOrder {
		for _, s := range stagesList {
			if s.ID == stageIndex {
				unwindStages[i] = s
				break
			}
		}
	}

	st := &StagedSync{
		stages:       stagesList,
		currentStage: 0,
		unwindOrder:  unwindStages,
	}

	return st
}

func (s *StagedSync) StageState(stage stages.SyncStage, db ethdb.KVGetter) (*StageState, error) {
	blockNum, err := stages.GetStageProgress(db, stage)
	if err != nil {
		return nil, err
	}
	return &StageState{s, stage, blockNum}, nil
}

func (s *StagedSync) Run(db ethdb.RwKV, tx ethdb.RwTx, firstCycle bool) error {
	var timings []interface{}
	for !s.IsDone() {
		if s.unwindPoint != nil {
			for i := len(s.unwindOrder) - 1; i >= 0; i-- {
				if err := s.SetCurrentStage(s.unwindOrder[i].ID); err != nil {
					return err
				}
				if s.unwindOrder[i].Disabled {
					continue
				}
				t := time.Now()
				if err := s.unwindStage(firstCycle, s.unwindOrder[i].ID, db, tx); err != nil {
					return err
				}
				timings = append(timings, "Unwind "+string(s.unwindOrder[i].ID), time.Since(t))
			}
			s.prevUnwindPoint = s.unwindPoint
			s.unwindPoint = nil
			s.badBlock = common.Hash{}
			if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
				return err
			}
		}

		_, stage := s.CurrentStage()

		if string(stage.ID) == debug.StopBeforeStage() { // stop process for debugging reasons
			log.Error("STOP_BEFORE_STAGE env flag forced to stop app")
			os.Exit(1)
		}

		if stage.Disabled {
			logPrefix := s.LogPrefix()
			message := fmt.Sprintf(
				"[%s] disabled. %s",
				logPrefix, stage.DisabledDescription,
			)

			log.Debug(message)

			s.NextStage()
			continue
		}

		t := time.Now()
		if err := s.runStage(stage, db, tx, firstCycle); err != nil {
			return err
		}
		timings = append(timings, string(stage.ID), time.Since(t))
	}

	if err := printLogs(tx, timings); err != nil {
		return err
	}
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

func (s *StagedSync) runStage(stage *Stage, db ethdb.RwKV, tx ethdb.RwTx, firstCycle bool) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stageState, err := s.StageState(stage.ID, tx)
	if err != nil {
		return err
	}

	if !useExternalTx {
		tx.Rollback()
		tx = nil
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

func (s *StagedSync) unwindStage(firstCycle bool, stageID stages.SyncStage, db ethdb.RwKV, tx ethdb.RwTx) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	start := time.Now()
	log.Info("Unwinding...", "stage", stageID)
	stage, err := s.StageByID(stageID)
	if err != nil {
		return err
	}
	if stage.Unwind == nil {
		return nil
	}
	var stageState *StageState
	stageState, err = s.StageState(stageID, tx)
	if err != nil {
		return err
	}
	unwind := s.NewUnwindState(stageID, *s.unwindPoint, stageState.BlockNumber)
	unwind.BadBlock = s.badBlock

	if stageState.BlockNumber <= unwind.UnwindPoint {
		return nil
	}
	if !useExternalTx {
		tx.Rollback()
		tx = nil
	}

	err = stage.Unwind(firstCycle, unwind, stageState, tx)
	if err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info("Unwinding... DONE!", "stage", string(unwind.ID))
	}
	return nil
}

func (s *StagedSync) DisableAllStages() {
	for i := range s.stages {
		s.stages[i].Disabled = true
	}
}

func (s *StagedSync) DisableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = true
		}
	}
}

func (s *StagedSync) EnableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = false
		}
	}
}

func (s *StagedSync) MockExecFunc(id stages.SyncStage, f ExecFunc) {
	for i := range s.stages {
		if s.stages[i].ID == id {
			s.stages[i].Forward = f
		}
	}
}
