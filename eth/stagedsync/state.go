package stagedsync

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

type State struct {
	unwindStack  *PersistentUnwindStack
	stages       []*Stage
	unwindOrder  []*Stage
	currentStage uint

	beforeStageRun    map[string]func() error
	onBeforeUnwind    func(stages.SyncStage) error
	beforeStageUnwind map[string]func() error
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
		if bytes.Equal(stage.ID, stage1) {
			idx1 = i
		}

		if bytes.Equal(stage.ID, stage2) {
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
		if bytes.Equal(stage.ID, stage1) {
			idx1 = i
		}

		if bytes.Equal(stage.ID, stage2) {
			idx2 = i
		}
	}

	return idx1 > idx2
}

func (s *State) GetLocalHeight(db ethdb.Getter) (uint64, error) {
	state, err := s.StageState(stages.Headers, db)
	return state.BlockNumber, err
}

func (s *State) UnwindTo(blockNumber uint64, db ethdb.Database) error {
	log.Info("UnwindTo", "block", blockNumber)
	for _, stage := range s.unwindOrder {
		if stage.Disabled {
			continue
		}
		if err := s.unwindStack.Add(UnwindState{stage.ID, blockNumber}, db); err != nil {
			return err
		}
	}
	return nil
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
		if bytes.Equal(stage.ID, id) {
			s.currentStage = uint(i)
			return nil
		}
	}
	return fmt.Errorf("stage not found with id: %v", id)
}

func (s *State) StageByID(id stages.SyncStage) (*Stage, error) {
	for _, stage := range s.stages {
		if bytes.Equal(stage.ID, id) {
			return stage, nil
		}
	}
	return nil, fmt.Errorf("stage not found with id: %v", id)
}

func NewState(stagesList []*Stage) *State {
	return &State{
		stages:            stagesList,
		currentStage:      0,
		unwindStack:       NewPersistentUnwindStack(),
		beforeStageRun:    make(map[string]func() error),
		beforeStageUnwind: make(map[string]func() error),
	}
}

func (s *State) LoadUnwindInfo(db ethdb.Getter) error {
	for _, stage := range s.unwindOrder {
		if err := s.unwindStack.AddFromDB(db, stage.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) StageState(stage stages.SyncStage, db ethdb.Getter) (*StageState, error) {
	blockNum, err := stages.GetStageProgress(db, stage)
	if err != nil {
		return nil, err
	}
	return &StageState{s, stage, blockNum}, nil
}

func (s *State) Run(db ethdb.GetterPutter, tx ethdb.GetterPutter) error {
	var timings []interface{}
	for !s.IsDone() {
		if !s.unwindStack.Empty() {
			for unwind := s.unwindStack.Pop(); unwind != nil; unwind = s.unwindStack.Pop() {
				if err := s.SetCurrentStage(unwind.Stage); err != nil {
					return err
				}
				if s.onBeforeUnwind != nil {
					if err := s.onBeforeUnwind(unwind.Stage); err != nil {
						return err
					}
				}
				if hook, ok := s.beforeStageUnwind[string(unwind.Stage)]; ok {
					if err := hook(); err != nil {
						return err
					}
				}
				t := time.Now()
				if err := s.UnwindStage(unwind, db, tx); err != nil {
					return err
				}
				timings = append(timings, "Unwind "+string(unwind.Stage), time.Since(t))
			}
			if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
				return err
			}
		}
		_, stage := s.CurrentStage()

		if hook, ok := s.beforeStageRun[string(stage.ID)]; ok {
			if err := hook(); err != nil {
				return err
			}
		}

		if stage.Disabled {
			logPrefix := s.LogPrefix()
			message := fmt.Sprintf(
				"[%s] disabled. %s",
				logPrefix, stage.DisabledDescription,
			)

			log.Info(message)

			s.NextStage()
			continue
		}

		t := time.Now()
		if err := s.runStage(stage, db, tx); err != nil {
			return err
		}
		timings = append(timings, string(stage.ID), time.Since(t))
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory", "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	log.Info("Timings", timings...)
	if err := printBucketsSize(tx.(ethdb.HasTx).Tx()); err != nil {
		return err
	}
	return nil
}

func printBucketsSize(tx ethdb.Tx) error {
	defer func(t time.Time) { fmt.Printf("state.go:219: %s\n", time.Since(t)) }(time.Now())
	if tx == nil {
		return nil
	}
	buckets, err := tx.(ethdb.BucketMigrator).ExistingBuckets()
	if err != nil {
		return err
	}
	sort.Strings(buckets)
	bucketSizes := make([]interface{}, 0, 2*len(buckets))
	for _, bucket := range buckets {
		sz, err1 := tx.BucketSize(bucket)
		if err1 != nil {
			return err1
		}
		if sz < uint64(10*datasize.GB) {
			continue
		}
		bucketSizes = append(bucketSizes, bucket, common.StorageSize(sz))
	}
	if len(bucketSizes) == 0 {
		return nil
	}
	sz, err1 := tx.BucketSize("freelist")
	if err1 != nil {
		return err1
	}
	bucketSizes = append(bucketSizes, "freelist", common.StorageSize(sz))
	log.Info("Tables", bucketSizes...)
	return nil
}

func (s *State) runStage(stage *Stage, db ethdb.Getter, tx ethdb.Getter) error {
	if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		db = tx
	}
	stageState, err := s.StageState(stage.ID, db)
	if err != nil {
		return err
	}

	start := time.Now()
	logPrefix := s.LogPrefix()
	if err = stage.ExecFunc(stageState, s); err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info(fmt.Sprintf("[%s] DONE", logPrefix), "in", time.Since(start))
	}
	return nil
}

func (s *State) UnwindStage(unwind *UnwindState, db ethdb.GetterPutter, tx ethdb.GetterPutter) error {
	if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		db = tx
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

	stageState, err := s.StageState(unwind.Stage, db)
	if err != nil {
		return err
	}

	if stageState.BlockNumber <= unwind.UnwindPoint {
		if err = unwind.Skip(db); err != nil {
			return err
		}
		return nil
	}

	err = stage.UnwindFunc(unwind, stageState)
	if err != nil {
		return err
	}

	if time.Since(start) > 30*time.Second {
		log.Info("Unwinding... DONE!", "stage", string(unwind.Stage))
	}
	return nil
}

func (s *State) DisableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if !bytes.Equal(s.stages[i].ID, id) {
				continue
			}
			s.stages[i].Disabled = true
		}
	}
}

func (s *State) EnableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if !bytes.Equal(s.stages[i].ID, id) {
				continue
			}
			s.stages[i].Disabled = false
		}
	}
}

func (s *State) MockExecFunc(id stages.SyncStage, f ExecFunc) {
	for i := range s.stages {
		if bytes.Equal(s.stages[i].ID, id) {
			s.stages[i].ExecFunc = f
		}
	}
}

func (s *State) BeforeStageRun(id stages.SyncStage, f func() error) {
	s.beforeStageRun[string(id)] = f
}

func (s *State) BeforeStageUnwind(id stages.SyncStage, f func() error) {
	s.beforeStageUnwind[string(id)] = f
}

func (s *State) OnBeforeUnwind(f func(id stages.SyncStage) error) {
	s.onBeforeUnwind = f
}
