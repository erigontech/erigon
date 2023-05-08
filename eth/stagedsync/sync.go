package stagedsync

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

type Sync struct {
	unwindPoint     *uint64 // used to run stages
	prevUnwindPoint *uint64 // used to get value from outside of staged sync after cycle (for example to notify RPCDaemon)
	badBlock        libcommon.Hash

	stages       []*Stage
	unwindOrder  []*Stage
	pruningOrder []*Stage
	currentStage uint
	timings      []Timing
	logPrefixes  []string
	logger       log.Logger
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
	return &UnwindState{id, unwindPoint, currentProgress, libcommon.Hash{}, s}
}

func (s *Sync) PruneStageState(id stages.SyncStage, forwardProgress uint64, tx kv.Tx, db kv.RwDB) (*PruneState, error) {
	var pruneProgress uint64
	var err error
	useExternalTx := tx != nil
	if useExternalTx {
		pruneProgress, err = stages.GetStagePruneProgress(tx, id)
		if err != nil {
			return nil, err
		}
	} else {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
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

func (s *Sync) UnwindTo(unwindPoint uint64, badBlock libcommon.Hash) {
	s.logger.Info("UnwindTo", "block", unwindPoint, "bad_block_hash", badBlock.String())
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
	return s.logPrefixes[s.currentStage]
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

func New(stagesList []*Stage, unwindOrder UnwindOrder, pruneOrder PruneOrder, logger log.Logger) *Sync {
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
	logPrefixes := make([]string, len(stagesList))
	for i := range stagesList {
		logPrefixes[i] = fmt.Sprintf("%d/%d %s", i+1, len(stagesList), stagesList[i].ID)
	}

	return &Sync{
		stages:       stagesList,
		currentStage: 0,
		unwindOrder:  unwindStages,
		pruningOrder: pruneStages,
		logPrefixes:  logPrefixes,
		logger:       logger,
	}
}

func (s *Sync) StageState(stage stages.SyncStage, tx kv.Tx, db kv.RoDB) (*StageState, error) {
	var blockNum uint64
	var err error
	useExternalTx := tx != nil
	if useExternalTx {
		blockNum, err = stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
	} else {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
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

func (s *Sync) RunUnwind(db kv.RwDB, tx kv.RwTx) error {
	if s.unwindPoint == nil {
		return nil
	}
	for j := 0; j < len(s.unwindOrder); j++ {
		if s.unwindOrder[j] == nil || s.unwindOrder[j].Disabled || s.unwindOrder[j].Unwind == nil {
			continue
		}
		if err := s.unwindStage(false, s.unwindOrder[j], db, tx); err != nil {
			return err
		}
	}
	s.prevUnwindPoint = s.unwindPoint
	s.unwindPoint = nil
	s.badBlock = libcommon.Hash{}
	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return err
	}
	return nil
}
func (s *Sync) Run(db kv.RwDB, tx kv.RwTx, firstCycle bool) error {
	s.prevUnwindPoint = nil
	s.timings = s.timings[:0]

	for !s.IsDone() {
		var badBlockUnwind bool
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
			if s.badBlock != (libcommon.Hash{}) {
				badBlockUnwind = true
			}
			s.badBlock = libcommon.Hash{}
			if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
				return err
			}
			// If there were unwinds at the start, a heavier but invalid chain may be present, so
			// we relax the rules for Stage1
			firstCycle = false
		}
		if badBlockUnwind {
			// If there was a bad block, the current step needs to complete, to send the corresponding reply to the Consensus Layer
			// Otherwise, the staged sync will get stuck in the Headers stage with "Waiting for Consensus Layer..."
			break
		}

		stage := s.stages[s.currentStage]

		if string(stage.ID) == dbg.StopBeforeStage() { // stop process for debugging reasons
			s.logger.Warn("STOP_BEFORE_STAGE env flag forced to stop app")
			return libcommon.ErrStopped
		}

		if stage.Disabled || stage.Forward == nil {
			s.logger.Trace(fmt.Sprintf("%s disabled. %s", stage.ID, stage.DisabledDescription))

			s.NextStage()
			continue
		}

		if err := s.runStage(stage, db, tx, firstCycle, badBlockUnwind); err != nil {
			return err
		}

		if string(stage.ID) == dbg.StopAfterStage() { // stop process for debugging reasons
			s.logger.Warn("STOP_AFTER_STAGE env flag forced to stop app")
			return libcommon.ErrStopped
		}

		s.NextStage()
	}

	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return err
	}

	s.currentStage = 0
	return nil
}
func (s *Sync) RunPrune(db kv.RwDB, tx kv.RwTx, firstCycle bool) error {
	s.timings = s.timings[:0]
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
	s.currentStage = 0
	return nil
}

func (s *Sync) PrintTimings() []interface{} {
	var logCtx []interface{}
	count := 0
	for i := range s.timings {
		if s.timings[i].took < 50*time.Millisecond {
			continue
		}
		count++
		if count == 50 {
			break
		}
		if s.timings[i].isUnwind {
			logCtx = append(logCtx, "Unwind "+string(s.timings[i].stage), s.timings[i].took.Truncate(time.Millisecond).String())
		} else if s.timings[i].isPrune {
			logCtx = append(logCtx, "Prune "+string(s.timings[i].stage), s.timings[i].took.Truncate(time.Millisecond).String())
		} else {
			logCtx = append(logCtx, string(s.timings[i].stage), s.timings[i].took.Truncate(time.Millisecond).String())
		}
	}
	return logCtx
}

func PrintTables(db kv.RoDB, tx kv.RwTx) []interface{} {
	if tx == nil {
		return nil
	}
	buckets := []string{
		kv.PlainState,
		kv.AccountChangeSet,
		kv.StorageChangeSet,
		kv.EthTx,
		kv.Log,
	}
	bucketSizes := make([]interface{}, 0, 2*(len(buckets)+2))
	for _, bucket := range buckets {
		sz, err1 := tx.BucketSize(bucket)
		if err1 != nil {
			return bucketSizes
		}
		bucketSizes = append(bucketSizes, bucket, libcommon.ByteCount(sz))
	}

	sz, err1 := tx.BucketSize("freelist")
	if err1 != nil {
		return bucketSizes
	}
	bucketSizes = append(bucketSizes, "FreeList", libcommon.ByteCount(sz))
	amountOfFreePagesInDb := sz / 4 // page_id encoded as bigEndian_u32
	if db != nil {
		bucketSizes = append(bucketSizes, "ReclaimableSpace", libcommon.ByteCount(amountOfFreePagesInDb*db.PageSize()))
	}
	tx.CollectMetrics()
	return bucketSizes
}

func (s *Sync) runStage(stage *Stage, db kv.RwDB, tx kv.RwTx, firstCycle bool, badBlockUnwind bool) (err error) {
	start := time.Now()
	stageState, err := s.StageState(stage.ID, tx, db)
	if err != nil {
		return err
	}

	if err = stage.Forward(firstCycle, badBlockUnwind, stageState, s, tx, s.logger); err != nil {
		wrappedError := fmt.Errorf("[%s] %w", s.LogPrefix(), err)
		s.logger.Debug("Error while executing stage", "err", wrappedError)
		return wrappedError
	}

	took := time.Since(start)
	logPrefix := s.LogPrefix()
	if took > 60*time.Second {
		s.logger.Info(fmt.Sprintf("[%s] DONE", logPrefix), "in", took)
	} else {
		s.logger.Debug(fmt.Sprintf("[%s] DONE", logPrefix), "in", took)
	}
	s.timings = append(s.timings, Timing{stage: stage.ID, took: took})
	return nil
}

func (s *Sync) unwindStage(firstCycle bool, stage *Stage, db kv.RwDB, tx kv.RwTx) error {
	start := time.Now()
	s.logger.Trace("Unwind...", "stage", stage.ID)
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

	err = stage.Unwind(firstCycle, unwind, stageState, tx, s.logger)
	if err != nil {
		return fmt.Errorf("[%s] %w", s.LogPrefix(), err)
	}

	took := time.Since(start)
	if took > 60*time.Second {
		logPrefix := s.LogPrefix()
		s.logger.Info(fmt.Sprintf("[%s] Unwind done", logPrefix), "in", took)
	}
	s.timings = append(s.timings, Timing{isUnwind: true, stage: stage.ID, took: took})
	return nil
}

func (s *Sync) pruneStage(firstCycle bool, stage *Stage, db kv.RwDB, tx kv.RwTx) error {
	start := time.Now()
	s.logger.Trace("Prune...", "stage", stage.ID)

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
		return fmt.Errorf("[%s] %w", s.LogPrefix(), err)
	}

	took := time.Since(start)
	if took > 60*time.Second {
		logPrefix := s.LogPrefix()
		s.logger.Info(fmt.Sprintf("[%s] Prune done", logPrefix), "in", took)
	}
	s.timings = append(s.timings, Timing{isPrune: true, stage: stage.ID, took: took})
	return nil
}

// DisableAllStages - including their unwinds
func (s *Sync) DisableAllStages() []stages.SyncStage {
	var backupEnabledIds []stages.SyncStage
	for i := range s.stages {
		if !s.stages[i].Disabled {
			backupEnabledIds = append(backupEnabledIds, s.stages[i].ID)
		}
	}
	for i := range s.stages {
		s.stages[i].Disabled = true
	}
	return backupEnabledIds
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
