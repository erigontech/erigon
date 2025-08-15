// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gofrs/flock"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/migrations"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/turbo/debug"
)

// Node is a container on which services can be registered.
type Node struct {
	config        *nodecfg.Config
	logger        log.Logger
	dirLock       *flock.Flock  // prevents concurrent use of instance directory
	stop          chan struct{} // Channel to wait for termination notifications
	startStopLock sync.Mutex    // Start/Stop are protected by an additional lock
	state         int           // Tracks state of node lifecycle

	lock       sync.Mutex
	lifecycles []Lifecycle // All registered backends, services, and auxiliary services that have a lifecycle

	databases []kv.Closer
}

const (
	initializingState = iota
	runningState
	closedState
)

// New creates a new P2P node, ready for protocol registration.
func New(ctx context.Context, conf *nodecfg.Config, logger log.Logger) (*Node, error) {
	// Copy config and resolve the datadir so future changes to the current
	// working directory don't affect the node.
	confCopy := *conf
	conf = &confCopy

	// Ensure that the instance name doesn't cause weird conflicts with
	// other files in the data directory.
	if strings.ContainsAny(conf.Name, `/\`) {
		return nil, errors.New(`Config.Name must not contain '/' or '\'`)
	}
	if strings.HasSuffix(conf.Name, ".ipc") {
		return nil, errors.New(`Config.Name cannot end in ".ipc"`)
	}

	node := &Node{
		config:    conf,
		logger:    logger,
		stop:      make(chan struct{}),
		databases: make([]kv.Closer, 0),
	}

	// Acquire the instance directory lock.
	if err := node.openDataDir(ctx); err != nil {
		return nil, err
	}

	return node, nil
}

// Start starts all registered lifecycles, RPC services and p2p networking.
// Node can only be started once.
func (n *Node) Start() error {
	n.startStopLock.Lock()
	defer n.startStopLock.Unlock()

	n.lock.Lock()
	switch n.state {
	case runningState:
		n.lock.Unlock()
		return ErrNodeRunning
	case closedState:
		n.lock.Unlock()
		return ErrNodeStopped
	}
	n.state = runningState
	lifecycles := make([]Lifecycle, len(n.lifecycles))
	copy(lifecycles, n.lifecycles)
	n.lock.Unlock()

	// Start all registered lifecycles.
	// preallocation leads to bugs here
	var started []Lifecycle //nolint:prealloc
	var err error
	for _, lifecycle := range lifecycles {
		if err = lifecycle.Start(); err != nil {
			break
		}
		started = append(started, lifecycle)
	}
	// Check if any lifecycle failed to start.
	if err != nil {
		stopErr := n.stopServices(started)
		if stopErr != nil {
			n.logger.Warn("Failed to doClose for this node", "err", stopErr)
		} //nolint:errcheck
		closeErr := n.doClose(nil)
		if closeErr != nil {
			n.logger.Warn("Failed to doClose for this node", "err", closeErr)
		}
	}
	return err
}

// Close stops the Node and releases resources acquired in
// Node constructor New.
func (n *Node) Close() error {
	n.startStopLock.Lock()
	defer n.startStopLock.Unlock()

	n.lock.Lock()
	state := n.state
	n.lock.Unlock()
	switch state {
	case initializingState:
		// The node was never started.
		return n.doClose(nil)
	case runningState:
		// The node was started, release resources acquired by Start().
		var errs []error
		if err := n.stopServices(n.lifecycles); err != nil {
			errs = append(errs, err)
		}
		return n.doClose(errs)
	case closedState:
		return ErrNodeStopped
	default:
		panic(fmt.Sprintf("node is in unknown state %d", state))
	}
}

// doClose releases resources acquired by New(), collecting errors.
func (n *Node) doClose(errs []error) error {
	// Close databases. This needs the lock because it needs to
	// synchronize with OpenDatabase*.
	n.lock.Lock()
	n.state = closedState
	for _, closer := range n.databases {
		closer.Close()
	}
	n.lock.Unlock()

	// Release instance directory lock.
	n.closeDataDir()

	// Unblock n.Wait.
	close(n.stop)

	// Report any errors that might have occurred.
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return fmt.Errorf("%v", errs)
	}
}

// containsLifecycle checks if 'lfs' contains 'l'.
func containsLifecycle(lfs []Lifecycle, l Lifecycle) bool {
	return slices.Contains(lfs, l)
}

// stopServices terminates running services, RPC and p2p networking.
// It is the inverse of Start.
func (n *Node) stopServices(running []Lifecycle) error {
	//n.stopRPC()

	// Stop running lifecycles in reverse order.
	failure := &StopError{Services: make(map[reflect.Type]error)}
	for i := len(running) - 1; i >= 0; i-- {
		if err := running[i].Stop(); err != nil {
			failure.Services[reflect.TypeOf(running[i])] = err
		}
	}

	if len(failure.Services) > 0 {
		return failure
	}
	return nil
}

func (n *Node) openDataDir(ctx context.Context) error {
	if n.config.Dirs.DataDir == "" {
		return nil // ephemeral
	}

	instdir := n.config.Dirs.DataDir
	for retry := 0; ; retry++ {
		l, locked, err := datadir.TryFlock(n.config.Dirs)
		if err != nil {
			return err
		}
		if !locked {
			if retry >= 10 {
				return fmt.Errorf("%w: %s", datadir.ErrDataDirLocked, instdir)
			}
			log.Error(datadir.ErrDataDirLocked.Error() + ", retry in 2 sec")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			continue
		}
		n.dirLock = l
		break
	}
	return nil
}

func (n *Node) closeDataDir() {
	// Release instance directory lock.
	if n.dirLock != nil {
		if err := n.dirLock.Unlock(); err != nil {
			n.logger.Error("Can't release datadir lock", "err", err)
		}
		n.dirLock = nil
	}
}

// Wait blocks until the node is closed.
func (n *Node) Wait() {
	<-n.stop
}

// RegisterLifecycle registers the given Lifecycle on the node.
func (n *Node) RegisterLifecycle(lifecycle Lifecycle) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.state != initializingState {
		panic("can't register lifecycle on running/stopped node")
	}
	if containsLifecycle(n.lifecycles, lifecycle) {
		panic(fmt.Sprintf("attempt to register lifecycle %T more than once", lifecycle))
	}
	n.lifecycles = append(n.lifecycles, lifecycle)
}

// Config returns the configuration of node.
func (n *Node) Config() *nodecfg.Config {
	return n.config
}

// DataDir retrieves the current datadir used by the protocol stack.
func (n *Node) DataDir() string {
	return n.config.Dirs.DataDir
}

func OpenDatabase(ctx context.Context, config *nodecfg.Config, label kv.Label, name string, readonly bool, logger log.Logger) (kv.RwDB, error) {
	switch label {
	case kv.ChainDB:
		name = "chaindata"
	case kv.TxPoolDB:
		name = "txpool"
	case kv.PolygonBridgeDB:
		name = "polygon-bridge"
	case kv.ConsensusDB:
		if len(name) == 0 {
			return nil, errors.New("expected a consensus name")
		}
	default:
		name = "test"
	}

	var db kv.RwDB
	if config.Dirs.DataDir == "" {
		db = memdb.New("", label)
		return db, nil
	}

	dbPath := filepath.Join(config.Dirs.DataDir, name)

	logger.Info("Opening Database", "label", name, "path", dbPath)
	openFunc := func(exclusive bool) (kv.RwDB, error) {
		roTxLimit := int64(32)
		if config.Http.DBReadConcurrency > 0 {
			roTxLimit = int64(config.Http.DBReadConcurrency)
		}
		roTxsLimiter := semaphore.NewWeighted(roTxLimit) // 1 less than max to allow unlocking to happen
		opts := mdbx.New(label, logger).
			Path(dbPath).
			GrowthStep(16 * datasize.MB).
			DBVerbosity(config.DatabaseVerbosity).RoTxsLimiter(roTxsLimiter).
			WriteMap(config.MdbxWriteMap).
			Readonly(readonly).
			Exclusive(exclusive)

		switch label {
		case kv.ChainDB:
			if config.MdbxPageSize.Bytes() > 0 {
				opts = opts.PageSize(config.MdbxPageSize)
			}
			if config.MdbxDBSizeLimit > 0 {
				opts = opts.MapSize(config.MdbxDBSizeLimit)
			}
			if config.MdbxGrowthStep > 0 {
				opts = opts.GrowthStep(config.MdbxGrowthStep)
			}
			opts = opts.DirtySpace(uint64(1024 * datasize.MB))
		case kv.ConsensusDB:
			if config.MdbxPageSize.Bytes() > 0 {
				opts = opts.PageSize(config.MdbxPageSize)
			}
			// Don't adjust up the consensus DB - this will lead to resource exhaustion lor large map sizes
			if config.MdbxDBSizeLimit > 0 && config.MdbxDBSizeLimit < mdbx.DefaultMapSize {
				opts = opts.MapSize(config.MdbxDBSizeLimit)
			}
			// Don't adjust up the consensus DB - to align with db size limit above
			if config.MdbxGrowthStep > 0 && config.MdbxGrowthStep < mdbx.DefaultGrowthStep {
				opts = opts.GrowthStep(config.MdbxGrowthStep)
			}
		default:
		}

		return opts.Open(ctx)
	}
	var err error
	db, err = openFunc(false)
	if err != nil {
		return nil, err
	}

	if label == kv.ChainDB {
		migrator := migrations.NewMigrator(label)
		if err := migrator.VerifyVersion(db, dbPath); err != nil {
			return nil, err
		}
		has, err := migrator.HasPendingMigrations(db)
		if err != nil {
			return nil, err
		}
		if has && !dbg.OnlyCreateDB {
			logger.Info("Re-Opening DB in exclusive mode to apply migrations")
			db.Close()
			db, err = openFunc(true)
			if err != nil {
				return nil, err
			}
			if err = migrator.Apply(db, config.Dirs.DataDir, dbPath, logger); err != nil {
				return nil, err
			}
			db.Close()
			db, err = openFunc(false)
			if err != nil {
				return nil, err
			}
		}
		if err := db.Update(context.Background(), func(tx kv.RwTx) (err error) {
			return rawdb.SetErigonVersion(tx, version.VersionKeyCreated)
		}); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// ResolvePath returns the absolute path of a resource in the instance directory.
func (n *Node) ResolvePath(x string) string {
	return n.config.ResolvePath(x)
}

func StartNode(stack *Node) {
	if err := stack.Start(); err != nil {
		utils.Fatalf("Error starting protocol stack: %v", err)
	}

	go debug.ListenSignals(stack, stack.logger)
}
