// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

type Sync struct {
	cfg             ethconfig.Sync
	unwindPoint     *uint64 // used to run stages
	prevUnwindPoint *uint64 // used to get value from outside staged sync after cycle (for example to notify RPCDaemon)
	unwindReason    UnwindReason
	posTransition   *uint64

	stages        []*Stage
	unwindOrder   []*Stage
	pruningOrder  []*Stage
	currentStage  uint
	timings       []Timing
	logPrefixes   []string
	logger        log.Logger
	stagesIdsList []string
	mode          stages.Mode
	metricsCache  metricsCache
}

type Timing struct {
	isUnwind bool
	isPrune  bool
	stage    stages.SyncStage
	took     time.Duration
}

func (s *Sync) Len() int {
	return len(s.stages)
}

func (s *Sync) Cfg() ethconfig.Sync { return s.cfg }

func (s *Sync) UnwindPoint() uint64 {
	return *s.unwindPoint
}

func (s *Sync) UnwindReason() UnwindReason {
	return s.unwindReason
}

func (s *Sync) PrevUnwindPoint() *uint64 {
	return s.prevUnwindPoint
}

func (s *Sync) NewUnwindState(id stages.SyncStage, unwindPoint, currentProgress uint64, initialCycle, firstCycle bool) *UnwindState {
	return &UnwindState{id, unwindPoint, currentProgress, UnwindReason{nil, nil}, s, CurrentSyncCycleInfo{initialCycle, firstCycle}}
}

// PruneStageState Get the current prune status from the DB
func (s *Sync) PruneStageState(id stages.SyncStage, forwardProgress uint64, tx kv.Tx, db kv.RwDB, initialCycle bool) (*PruneState, error) {
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

	return &PruneState{id, forwardProgress, pruneProgress, s, CurrentSyncCycleInfo{initialCycle, false}}, nil
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

func (s *Sync) HasUnwindPoint() bool { return s.unwindPoint != nil }
func (s *Sync) UnwindTo(unwindPoint uint64, reason UnwindReason, tx kv.Tx) error {
	if tx != nil {
		if aggTx := state.AggTx(tx); aggTx != nil {
			// protect from too far unwind
			unwindPointWithCommitment, ok, err := aggTx.CanUnwindBeforeBlockNum(unwindPoint, tx)
			// Ignore in the case that snapshots are ahead of commitment, it will be resolved later.
			// This can be a problem if snapshots include a wrong chain so it is ok to ignore it.
			if errors.Is(err, state.ErrBehindCommitment) {
				return nil
			}
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("too far unwind. requested=%d, minAllowed=%d", unwindPoint, unwindPointWithCommitment)
			}
			unwindPoint = unwindPointWithCommitment
		}
	}

	if reason.Block != nil {
		s.logger.Debug("UnwindTo", "block", unwindPoint, "block_hash", reason.Block.String(), "err", reason.Err, "stack", dbg.Stack())
	} else {
		s.logger.Debug("UnwindTo", "block", unwindPoint, "stack", dbg.Stack())
	}

	s.unwindPoint = &unwindPoint
	s.unwindReason = reason
	return nil
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

func (s *Sync) StagesIdsList() []string {
	if s == nil {
		return []string{}
	}
	return s.stagesIdsList
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

func New(cfg ethconfig.Sync, stagesList []*Stage, unwindOrder UnwindOrder, pruneOrder PruneOrder, logger log.Logger, mode stages.Mode) *Sync {
	stageMap := make(map[stages.SyncStage]*Stage, len(stagesList))
	for _, s := range stagesList {
		stageMap[s.ID] = s
	}

	// on non-Polygon chains, WitnessProcessing stage is not run
	var filteredUnwindOrder UnwindOrder
	for _, stageIndex := range unwindOrder {
		if _, exists := stageMap[stageIndex]; exists {
			filteredUnwindOrder = append(filteredUnwindOrder, stageIndex)
		}
	}

	var filteredPruneOrder PruneOrder
	for _, stageIndex := range pruneOrder {
		if _, exists := stageMap[stageIndex]; exists {
			filteredPruneOrder = append(filteredPruneOrder, stageIndex)
		}
	}

	unwindStages := make([]*Stage, len(filteredUnwindOrder))
	for i, stageIndex := range filteredUnwindOrder {
		unwindStages[i] = stageMap[stageIndex]
	}

	pruneStages := make([]*Stage, len(filteredPruneOrder))
	for i, stageIndex := range filteredPruneOrder {
		pruneStages[i] = stageMap[stageIndex]
	}

	logPrefixes := make([]string, len(stagesList))
	stagesIdsList := make([]string, len(stagesList))
	for i := range stagesList {
		logPrefixes[i] = fmt.Sprintf("%d/%d %s", i+1, len(stagesList), stagesList[i].ID)
		stagesIdsList[i] = string(stagesList[i].ID)
	}

	return &Sync{
		cfg:           cfg,
		stages:        stagesList,
		currentStage:  0,
		unwindOrder:   unwindStages,
		pruningOrder:  pruneStages,
		logPrefixes:   logPrefixes,
		logger:        logger,
		stagesIdsList: stagesIdsList,
		mode:          mode,
		metricsCache:  newMetricsCache(),
	}
}

func (s *Sync) StageState(stage stages.SyncStage, tx kv.Tx, db kv.RoDB, initialCycle, firstCycle bool) (*StageState, error) {
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

	return &StageState{s, stage, blockNum, CurrentSyncCycleInfo{initialCycle, firstCycle}}, nil
}

func (s *Sync) RunUnwind(db kv.RwDB, txc wrap.TxContainer) error {
	if s.unwindPoint == nil {
		return nil
	}
	for j := 0; j < len(s.unwindOrder); j++ {
		if s.unwindOrder[j] == nil || s.unwindOrder[j].Disabled || s.unwindOrder[j].Unwind == nil {
			continue
		}
		if err := s.unwindStage(false, s.unwindOrder[j], db, txc); err != nil {
			return err
		}
	}
	s.prevUnwindPoint = s.unwindPoint
	s.unwindPoint = nil
	s.unwindReason = UnwindReason{}
	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return err
	}
	return nil
}

func (s *Sync) RunNoInterrupt(db kv.RwDB, txc wrap.TxContainer) (bool, error) {
	var hasMore bool
	initialCycle, firstCycle := false, false
	s.prevUnwindPoint = nil
	s.timings = s.timings[:0]

	for !s.IsDone() {
		var badBlockUnwind bool
		if s.unwindPoint != nil {
			for j := 0; j < len(s.unwindOrder); j++ {
				if s.unwindOrder[j] == nil || s.unwindOrder[j].Disabled || s.unwindOrder[j].Unwind == nil {
					continue
				}
				if err := s.unwindStage(initialCycle, s.unwindOrder[j], db, txc); err != nil {
					return false, err
				}
			}
			s.prevUnwindPoint = s.unwindPoint
			s.unwindPoint = nil
			if s.unwindReason.IsBadBlock() {
				badBlockUnwind = true
			}
			s.unwindReason = UnwindReason{}
			if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
				return false, err
			}
			// If there were unwinds at the start, a heavier but invalid chain may be present, so
			// we relax the rules for Stage1
			initialCycle = false
		}

		stage := s.stages[s.currentStage]

		if string(stage.ID) == dbg.StopBeforeStage() { // stop process for debugging reasons
			s.logger.Warn("STOP_BEFORE_STAGE env flag forced to stop app")
			return false, common.ErrStopped
		}

		if stage.Disabled || stage.Forward == nil {
			s.logger.Trace(fmt.Sprintf("%s disabled. %s", stage.ID, stage.DisabledDescription))

			s.NextStage()
			continue
		}

		stageHasMore, err := s.runStage(stage, db, txc, initialCycle, firstCycle, badBlockUnwind)
		if err != nil {
			return false, err
		}
		if stageHasMore {
			hasMore = true
		}

		if string(stage.ID) == dbg.StopAfterStage() { // stop process for debugging reasons
			s.logger.Warn("STOP_AFTER_STAGE env flag forced to stop app")
			return false, common.ErrStopped
		}

		if string(stage.ID) == s.cfg.BreakAfterStage { // break process loop
			s.logger.Warn("--sync.loop.break.after caused stage break")
			break
		}

		s.NextStage()
	}

	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return false, err
	}

	s.currentStage = 0
	return hasMore, nil
}

// ErrLoopExhausted is used to allow the sync loop to continue when one of the stages has thrown it due to reaching
// some loop iteration limit.
type ErrLoopExhausted struct {
	From   uint64
	To     uint64
	Reason string
}

func (e *ErrLoopExhausted) Error() string {
	return fmt.Sprintf("loop exhausted: from=%d, to=%d, reason=%s", e.From, e.To, e.Reason)
}

func (e *ErrLoopExhausted) Is(err error) bool {
	var errExhausted *ErrLoopExhausted
	return errors.As(err, &errExhausted)
}

func (s *Sync) Run(db kv.RwDB, txc wrap.TxContainer, initialCycle, firstCycle bool) (bool, error) {
	s.prevUnwindPoint = nil
	s.timings = s.timings[:0]

	hasMore := false
	for !s.IsDone() {
		var badBlockUnwind bool
		if s.unwindPoint != nil {
			for j := 0; j < len(s.unwindOrder); j++ {
				if s.unwindOrder[j] == nil || s.unwindOrder[j].Disabled || s.unwindOrder[j].Unwind == nil {
					continue
				}
				if err := s.unwindStage(initialCycle, s.unwindOrder[j], db, txc); err != nil {
					return false, err
				}
			}
			s.prevUnwindPoint = s.unwindPoint
			s.unwindPoint = nil
			if s.unwindReason.IsBadBlock() {
				badBlockUnwind = true
			}
			s.unwindReason = UnwindReason{}
			if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
				return false, err
			}
			// If there were unwinds at the start, a heavier but invalid chain may be present, so
			// we relax the rules for Stage1
			initialCycle = false
		}
		if badBlockUnwind {
			// If there was a bad block, the current step needs to complete, to send the corresponding reply to the Consensus Layer
			// Otherwise, the staged sync will get stuck in the Headers stage with "Waiting for Consensus Layer..."
			break
		}

		stage := s.stages[s.currentStage]

		if string(stage.ID) == dbg.StopBeforeStage() { // stop process for debugging reasons
			s.logger.Warn("STOP_BEFORE_STAGE env flag forced to stop app")
			return false, common.ErrStopped
		}

		if stage.Disabled || stage.Forward == nil {
			s.logger.Trace(fmt.Sprintf("%s disabled. %s", stage.ID, stage.DisabledDescription))
			s.NextStage()
			continue
		}
		stageHasMore, err := s.runStage(stage, db, txc, initialCycle, firstCycle, badBlockUnwind)
		if err != nil {
			return false, err
		}
		if stageHasMore {
			hasMore = true
		}

		if string(stage.ID) == dbg.StopAfterStage() { // stop process for debugging reasons
			s.logger.Warn("STOP_AFTER_STAGE env flag forced to stop app")
			return false, common.ErrStopped
		}

		if string(stage.ID) == s.cfg.BreakAfterStage { // break process loop
			s.logger.Warn("--sync.loop.break.after caused stage break")
			if s.posTransition != nil {
				ptx := txc.Tx

				if ptx == nil {
					if tx, err := db.BeginRw(context.Background()); err == nil {
						ptx = tx
						defer tx.Rollback()
					}
				}

				if ptx != nil {
					if progress, err := stages.GetStageProgress(ptx, stage.ID); err == nil {
						hasMore = progress < *s.posTransition
					}
				}
			} else {
				hasMore = true
			}
			break
		}

		s.NextStage()
	}

	if err := s.SetCurrentStage(s.stages[0].ID); err != nil {
		return false, err
	}

	s.currentStage = 0
	return hasMore, nil
}

// RunPrune pruning for stages as per the defined pruning order, if enabled for that stage
func (s *Sync) RunPrune(db kv.RwDB, tx kv.RwTx, initialCycle bool) error {
	s.timings = s.timings[:0]
	for i := 0; i < len(s.pruningOrder); i++ {
		if s.pruningOrder[i] == nil || s.pruningOrder[i].Disabled || s.pruningOrder[i].Prune == nil {
			continue
		}
		if err := s.pruneStage(initialCycle, s.pruningOrder[i], db, tx); err != nil {
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
		if s.timings[i].took < 100*time.Millisecond {
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

func (s *Sync) runStage(stage *Stage, db kv.RwDB, txc wrap.TxContainer, initialCycle, firstCycle bool, badBlockUnwind bool) (bool, error) {
	start := time.Now()
	s.logger.Debug(fmt.Sprintf("[%s] Starting Stage run", s.LogPrefix()))
	stageState, err := s.StageState(stage.ID, txc.Tx, db, initialCycle, firstCycle)
	if err != nil {
		return false, err
	}

	if err = stage.Forward(badBlockUnwind, stageState, s, txc, s.logger); err != nil {
		var errExhausted *ErrLoopExhausted
		if errors.As(err, &errExhausted) {
			s.logger.Debug(fmt.Sprintf("[%s] loop exhausted", s.LogPrefix()), "msg", err.Error())
			s.logRunStageDone(stageState, start)
			return true, nil
		}
		s.logger.Debug(fmt.Sprintf("[%s] error while executing stage", s.LogPrefix()), "err", err)
		return false, fmt.Errorf("[%s] %w", s.LogPrefix(), err)
	}

	s.logRunStageDone(stageState, start)
	return false, nil
}

func (s *Sync) logRunStageDone(stageState *StageState, start time.Time) {
	took := time.Since(start)
	logPrefix := s.LogPrefix()
	if took > 60*time.Second {
		s.logger.Info(fmt.Sprintf("[%s] DONE", logPrefix), "in", took, "block", stageState.BlockNumber)
	} else {
		s.logger.Debug(fmt.Sprintf("[%s] DONE", logPrefix), "in", took)
	}
	s.timings = append(s.timings, Timing{stage: stageState.ID, took: took})
	s.metricsCache.stageRunDurationSummary(stageState.ID).Observe(took.Seconds())
}

func (s *Sync) unwindStage(initialCycle bool, stage *Stage, db kv.RwDB, txc wrap.TxContainer) error {
	start := time.Now()
	stageState, err := s.StageState(stage.ID, txc.Tx, db, initialCycle, false)
	if err != nil {
		return err
	}

	unwind := s.NewUnwindState(stage.ID, *s.unwindPoint, stageState.BlockNumber, initialCycle, false)
	unwind.Reason = s.unwindReason

	if stageState.BlockNumber <= unwind.UnwindPoint {
		return nil
	}

	if err = s.SetCurrentStage(stage.ID); err != nil {
		return err
	}

	err = stage.Unwind(unwind, stageState, txc, s.logger)
	if err != nil {
		return fmt.Errorf("[%s] %w", s.LogPrefix(), err)
	}

	took := time.Since(start)
	if took > 60*time.Second {
		logPrefix := s.LogPrefix()
		s.logger.Info(fmt.Sprintf("[%s] Unwind done", logPrefix), "in", took)
	}
	s.timings = append(s.timings, Timing{isUnwind: true, stage: stage.ID, took: took})
	s.metricsCache.stageUnwindDurationSummary(stage.ID).Observe(took.Seconds())
	return nil
}

// Run the pruning function for the given stage
func (s *Sync) pruneStage(initialCycle bool, stage *Stage, db kv.RwDB, tx kv.RwTx) error {
	start := time.Now()
	stageState, err := s.StageState(stage.ID, tx, db, initialCycle, false)
	if err != nil {
		return err
	}

	pruneState, err := s.PruneStageState(stage.ID, stageState.BlockNumber, tx, db, initialCycle)
	if err != nil {
		return err
	}
	if err = s.SetCurrentStage(stage.ID); err != nil {
		return err
	}

	err = stage.Prune(pruneState, tx, s.logger)
	if err != nil {
		return fmt.Errorf("[%s] %w", s.LogPrefix(), err)
	}

	took := time.Since(start)
	if took > 30*time.Second {
		s.logger.Info(fmt.Sprintf("[%s] Prune done", s.LogPrefix()), "in", took)
	} else {
		s.logger.Debug(fmt.Sprintf("[%s] Prune done", s.LogPrefix()), "in", took)
	}
	s.timings = append(s.timings, Timing{isPrune: true, stage: stage.ID, took: took})
	s.metricsCache.stagePruneDurationSummary(stage.ID).Observe(took.Seconds())
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
