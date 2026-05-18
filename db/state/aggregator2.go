package state

import (
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

// AggOpts is an Aggregator builder and contains only runtime-changeable configs (which may vary between Erigon nodes)
type AggOpts struct { //nolint:gocritic
	dirs                            datadir.Dirs
	logger                          log.Logger
	stepSize                        uint64 // != 0 mean override erigondb.toml settings
	stepsInFrozenFile               uint64 // != 0 mean override erigondb.toml settings
	erigondbDomainStepsInFrozenFile uint64
	reorgBlockDepth                 uint64

	genSaltIfNeed  bool
	disableFsync   bool // for tests speed
	disableHistory bool // for temp/inmem aggregator instances
}

func New(dirs datadir.Dirs) AggOpts { //nolint:gocritic
	return AggOpts{ //Defaults
		logger:          log.Root(),
		dirs:            dirs,
		reorgBlockDepth: dbg.MaxReorgDepth,
	}
}

func NewTest(dirs datadir.Dirs) AggOpts { //nolint:gocritic
	return New(dirs).DisableFsync().GenSaltIfNeed(true).ReorgBlockDepth(0).StepSize(config3.DefaultStepSize).StepsInFrozenFile(config3.DefaultStepsInFrozenFile)
}

func (opts AggOpts) Open(ctx context.Context, db kv.RoDB) (*Aggregator, error) { //nolint:gocritic
	//TODO: rename `OpenFolder` to `ReopenFolder`
	salt, err := GetStateIndicesSalt(opts.dirs, opts.genSaltIfNeed, opts.logger)
	if err != nil {
		return nil, err
	}

	a, err := newAggregator(ctx, opts.dirs, opts.reorgBlockDepth, db, opts.logger)
	if err != nil {
		return nil, err
	}

	a.stepSize.Store(opts.stepSize)
	a.stepsInFrozenFile.Store(opts.stepsInFrozenFile)
	a.erigondbDomainStepsInFrozenFile = opts.erigondbDomainStepsInFrozenFile

	a.disableHistory = opts.disableHistory
	a.disableFsync = opts.disableFsync

	a.savedSalt = salt

	if err := a.ConfigureDomains(); err != nil {
		return nil, err
	}

	return a, nil
}

func (opts AggOpts) MustOpen(ctx context.Context, db kv.RoDB) *Aggregator { //nolint:gocritic
	agg, err := opts.Open(ctx, db)
	if err != nil {
		panic(fmt.Errorf("fail to open mdbx: %w", err))
	}
	return agg
}

// Setters

func (opts AggOpts) StepSize(s uint64) AggOpts { opts.stepSize = s; return opts } //nolint:gocritic
func (opts AggOpts) StepsInFrozenFile(steps uint64) AggOpts { //nolint:gocritic
	opts.stepsInFrozenFile = steps
	return opts
}

// ErigondbDomainStepsInFrozenFile sets the domain-only cap override (see
// Aggregator.erigondbDomainStepsInFrozenFile). 0 clears the override;
// config3.UnboundedDomainMerge disables the cap; any other value replaces stepsInFrozenFile
// for domain merges only.
func (opts AggOpts) ErigondbDomainStepsInFrozenFile(steps uint64) AggOpts { //nolint:gocritic
	opts.erigondbDomainStepsInFrozenFile = steps
	return opts
}
func (opts AggOpts) ReorgBlockDepth(d uint64) AggOpts { //nolint:gocritic
	opts.reorgBlockDepth = d
	return opts
}
func (opts AggOpts) GenSaltIfNeed(v bool) AggOpts { opts.genSaltIfNeed = v; return opts }     //nolint:gocritic
func (opts AggOpts) Logger(l log.Logger) AggOpts  { opts.logger = l; return opts }            //nolint:gocritic
func (opts AggOpts) DisableFsync() AggOpts        { opts.disableFsync = true; return opts }   //nolint:gocritic
func (opts AggOpts) DisableHistory() AggOpts      { opts.disableHistory = true; return opts } //nolint:gocritic

// WithErigonDBSettings assigns pre-resolved DB settings (stepSize, stepsInFrozenFile).
func (opts AggOpts) WithErigonDBSettings(s *ErigonDBSettings) AggOpts { //nolint:gocritic
	opts.stepSize = s.StepSize
	opts.stepsInFrozenFile = s.StepsInFrozenFile
	return opts
}

type workersCfg struct {
	mu              sync.Mutex
	allowEditing    bool // false while a long op holds the lock; Preset* writes are no-ops
	merge           int  // usually 1
	collateAndBuild int
}

func (w *workersCfg) getMerge() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.merge
}

func (w *workersCfg) setMerge(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.allowEditing {
		w.merge = n
	}
}

func (w *workersCfg) getCollateAndBuild() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.collateAndBuild
}

func (w *workersCfg) setCollateAndBuild(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.allowEditing {
		w.collateAndBuild = n
	}
}

// trySet runs fn under mu only if allowEditing is true.
func (w *workersCfg) trySet(fn func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.allowEditing {
		fn()
	}
}

func (w *workersCfg) lockEditing() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.allowEditing = false
}

func (w *workersCfg) unlockEditing() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.allowEditing = true
}
