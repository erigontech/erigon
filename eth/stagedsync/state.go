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

type State struct {
	unwindStack  *PersistentUnwindStack
	stages       []*Stage
	unwindOrder  []*Stage
	currentStage uint
}

func (s *State) Len() int {
	return len(s.stages)
}

func (s *State) NextStage() {
	if s == nil {
		return
	}
	s.currentStage++
}

// IsBefore returns true if stage1 goes before stage2 in staged sync
func (s *State) IsBefore(stage1, stage2 stages.SyncStage) bool {
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
func (s *State) IsAfter(stage1, stage2 stages.SyncStage) bool {
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

func (s *State) GetLocalHeight(db ethdb.KVGetter) (uint64, error) {
	state, err := s.StageState(stages.Headers, db)
	return state.BlockNumber, err
}

func (s *State) UnwindTo(blockNumber uint64, tx TxOrDb, badBlock common.Hash) error {
	log.Info("UnwindTo", "block", blockNumber, "bad_block_hash", badBlock)
	for _, stage := range s.unwindOrder {
		if stage.Disabled {
			continue
		}
		if err := s.unwindStack.Add(UnwindState{stage.ID, blockNumber, badBlock}, tx); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) GetUnwindTo(db ethdb.KVGetter) (uint64, error) {
	return stages.GetStageUnwind(db, stages.Finish)
}

func (s *State) IsDone() bool {
	return s.currentStage >= uint(len(s.stages)) && s.unwindStack.Empty()
}

func (s *State) CurrentStage() (uint, *Stage) {
	return s.currentStage, s.stages[s.currentStage]
}

func (s *State) LogPrefix() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%d/%d %s", s.currentStage+1, s.Len(), s.stages[s.currentStage].ID)
}

func (s *State) SetCurrentStage(id stages.SyncStage) error {
	for i, stage := range s.stages {
		if stage.ID == id {
			s.currentStage = uint(i)
			return nil
		}
	}
	return fmt.Errorf("stage not found with id: %v", id)
}

func (s *State) StageByID(id stages.SyncStage) (*Stage, error) {
	for _, stage := range s.stages {
		if stage.ID == id {
			return stage, nil
		}
	}
	return nil, fmt.Errorf("stage not found with id: %v", id)
}

func NewState(stagesList []*Stage) *State {
	return &State{
		stages:       stagesList,
		currentStage: 0,
		unwindStack:  NewPersistentUnwindStack(),
	}
}

func (s *State) LoadUnwindInfo(db ethdb.KVGetter) error {
	for _, stage := range s.unwindOrder {
		if err := s.unwindStack.AddFromDB(db, stage.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) StageState(stage stages.SyncStage, db ethdb.KVGetter) (*StageState, error) {
	blockNum, err := stages.GetStageProgress(db, stage)
	if err != nil {
		return nil, err
	}
	return &StageState{s, stage, blockNum}, nil
}

func (s *State) Run(db ethdb.RwKV, tx ethdb.RwTx) error {
	var timings []interface{}
	for !s.IsDone() {
		if !s.unwindStack.Empty() {
			for unwind := s.unwindStack.Pop(); unwind != nil; unwind = s.unwindStack.Pop() {
				if err := s.SetCurrentStage(unwind.Stage); err != nil {
					return err
				}
				t := time.Now()
				if err := s.unwindStage(unwind, db, tx); err != nil {
					return err
				}
				timings = append(timings, "Unwind "+string(unwind.Stage), time.Since(t))
			}
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
		if err := s.runStage(stage, db, tx); err != nil {
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

func (s *State) runStage(stage *Stage, db ethdb.RwKV, tx ethdb.RwTx) error {
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
	if err = stage.ExecFunc(stageState, s, tx); err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info(fmt.Sprintf("[%s] DONE", logPrefix), "in", time.Since(start))
	}
	return nil
}

func (s *State) unwindStage(unwind *UnwindState, db ethdb.RwKV, tx ethdb.RwTx) error {
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
	log.Info("Unwinding...", "stage", string(unwind.Stage))
	stage, err := s.StageByID(unwind.Stage)
	if err != nil {
		return err
	}
	if stage.UnwindFunc == nil {
		return nil
	}
	var stageState *StageState
	stageState, err = s.StageState(unwind.Stage, tx)
	if err != nil {
		return err
	}

	if stageState.BlockNumber <= unwind.UnwindPoint {
		if err = unwind.Skip(tx); err != nil {
			return err
		}
		return nil
	}
	if !useExternalTx {
		tx.Rollback()
		tx = nil
	}

	err = stage.UnwindFunc(unwind, stageState, tx)
	if err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info("Unwinding... DONE!", "stage", string(unwind.Stage))
	}
	return nil
}

func (s *State) DisableAllStages() {
	for i := range s.stages {
		s.stages[i].Disabled = true
	}
}

func (s *State) DisableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = true
		}
	}
}

func (s *State) EnableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = false
		}
	}
}

func (s *State) MockExecFunc(id stages.SyncStage, f ExecFunc) {
	for i := range s.stages {
		if s.stages[i].ID == id {
			s.stages[i].ExecFunc = f
		}
	}
}
