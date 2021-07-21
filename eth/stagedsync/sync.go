package stagedsync

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

type Sync struct {
	unwindPoint     *uint64 // used to run stages
	prevUnwindPoint *uint64 // used to get value from outside of staged sync after cycle (for example to notify RPCDaemon)
	badBlock        common.Hash

	stages       []*Stage
	unwindOrder  []*Stage
	pruningOrder []*Stage
	currentStage uint
	timings      []Timing
}
type Timing struct {
	isUnwind bool
	isPrune  bool
	stage    stages.SyncStage
	took     time.Duration
}

func (s *Sync) Len() int                 { return len(s.stages) }
func (s *Sync) PrevUnwindPoint() *uint64 { return s.prevUnwindPoint }

func (s *Sync) NewUnwindState(id stages.SyncStage, unwindPoint, currentProgress uint64) *UnwindState {
	return &UnwindState{id, unwindPoint, currentProgress, common.Hash{}, s}
}

func (s *Sync) PruneStageState(id stages.SyncStage, forwardProgress uint64, tx ethdb.Tx, db ethdb.RwKV) (*PruneState, error) {
	var pruneProgress uint64
	var err error
	useExternalTx := tx != nil
	if useExternalTx {
		pruneProgress, err = stages.GetStagePruneProgress(tx, id)
		if err != nil {
			return nil, err
		}
	} else {
		if err = db.View(context.Background(), func(tx ethdb.Tx) error {
			pruneProgress, err = stages.GetStagePruneProgress(tx, id)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	return &PruneState{id, forwardProgress, pruneProgress, s}, nil
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
		pruningOrder: pruneStages,
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
	s.timings = s.timings[:0]
	for !s.IsDone() {
		if s.unwindPoint != nil {
			for j := 0; j < len(s.unwindOrder); j++ {
				if s.unwindOrder[j] == nil || s.unwindOrder[j].Disabled || s.unwindOrder[j].Unwind == nil {
					continue
				}
				if err := s.unwindStage(firstCycle, s.unwindOrder[j], db, tx); err != nil {
					return err
				}
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

		if err := s.runStage(stage, db, tx, firstCycle); err != nil {
			return err
		}

		s.NextStage()
	}

	for i := 0; i < len(s.pruningOrder); i++ {
		if s.pruningOrder[i] == nil || s.pruningOrder[i].Disabled || s.pruningOrder[i].Prune == nil {
			continue
		}
		if err := s.pruneStage(firstCycle, s.pruningOrder[i], db, tx); err != nil {
			return err
		}
	}
	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return err
	}

	if err := printLogs(tx, s.timings); err != nil {
		return err
	}
	s.currentStage = 0
	return nil
}

func printLogs(tx ethdb.RwTx, timings []Timing) error {
	var logCtx []interface{}
	count := 0
	for i := range timings {
		if timings[i].took < 10*time.Millisecond {
			continue
		}
		count++
		if count == 50 {
			break
		}
		if timings[i].isUnwind {
			logCtx = append(logCtx, "Unwind "+string(timings[i].stage), timings[i].took.String())
		} else if timings[i].isPrune {
			logCtx = append(logCtx, "Prune "+string(timings[i].stage), timings[i].took.String())
		} else {
			logCtx = append(logCtx, string(timings[i].stage), timings[i].took.String())
		}
	}
	if len(logCtx) > 0 {
		log.Info("Timings (slower than 10ms)", logCtx...)
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
	start := time.Now()
	stageState, err := s.StageState(stage.ID, tx, db)
	if err != nil {
		return err
	}

	logPrefix := s.LogPrefix()
	if err = stage.Forward(firstCycle, stageState, s, tx); err != nil {
		return err
	}

	t := time.Since(start)
	if t > 60*time.Second {
		log.Info(fmt.Sprintf("[%s] DONE", logPrefix), "in", t)
	}
	return nil
}

func (s *Sync) unwindStage(firstCycle bool, stage *Stage, db ethdb.RwKV, tx ethdb.RwTx) error {
	t := time.Now()
	log.Debug("Unwind...", "stage", stage.ID)
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

	took := time.Since(t)
	if took > 60*time.Second {
		log.Info("Unwind... DONE!", "stage", string(unwind.ID), "in", took)
	}
	s.timings = append(s.timings, Timing{isUnwind: true, stage: stage.ID, took: time.Since(t)})
	return nil
}

func (s *Sync) pruneStage(firstCycle bool, stage *Stage, db ethdb.RwKV, tx ethdb.RwTx) error {
	t := time.Now()
	log.Debug("Prune...", "stage", stage.ID)

	stageState, err := s.StageState(stage.ID, tx, db)
	if err != nil {
		return err
	}

	prune, err := s.PruneStageState(stage.ID, stageState.BlockNumber, tx, db)
	if err != nil {
		return err
	}
	if err = s.SetCurrentStage(stage.ID); err != nil {
		return err
	}

	err = stage.Prune(firstCycle, prune, tx)
	if err != nil {
		return err
	}

	took := time.Since(t)
	if took > 60*time.Second {
		log.Info("Prune... DONE!", "stage", string(prune.ID), "in", took)
	}
	s.timings = append(s.timings, Timing{isPrune: true, stage: stage.ID, took: time.Since(t)})
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
