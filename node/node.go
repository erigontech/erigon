// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"golang.org/x/sync/semaphore"

	"github.com/gofrs/flock"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/log/v3"
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
func New(conf *nodecfg.Config, logger log.Logger) (*Node, error) {
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
	if err := node.openDataDir(); err != nil {
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
	for _, obj := range lfs {
		if obj == l {
			return true
		}
	}
	return false
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

func (n *Node) openDataDir() error {
	if n.config.Dirs.DataDir == "" {
		return nil // ephemeral
	}

	instdir := n.config.Dirs.DataDir
	if err := os.MkdirAll(instdir, 0700); err != nil {
		return err
	}
	// Lock the instance directory to prevent concurrent use by another instance as well as
	// accidental use of the instance directory as a database.
	l := flock.New(filepath.Join(instdir, "LOCK"))
	locked, err := l.TryLock()
	if err != nil {
		return convertFileLockError(err)
	}
	if !locked {
		return fmt.Errorf("%w: %s", ErrDataDirUsed, instdir)
	}
	n.dirLock = l
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

func OpenDatabase(config *nodecfg.Config, label kv.Label, logger log.Logger) (kv.RwDB, error) {
	var name string
	switch label {
	case kv.ChainDB:
		name = "chaindata"
	case kv.TxPoolDB:
		name = "txpool"
	default:
		name = "test"
	}
	var db kv.RwDB
	if config.Dirs.DataDir == "" {
		db = memdb.New("")
		return db, nil
	}

	dbPath := filepath.Join(config.Dirs.DataDir, name)
	var openFunc func(exclusive bool) (kv.RwDB, error)
	logger.Info("Opening Database", "label", name, "path", dbPath)
	openFunc = func(exclusive bool) (kv.RwDB, error) {
		roTxLimit := int64(32)
		if config.Http.DBReadConcurrency > 0 {
			roTxLimit = int64(config.Http.DBReadConcurrency)
		}
		roTxsLimiter := semaphore.NewWeighted(roTxLimit) // 1 less than max to allow unlocking to happen
		opts := mdbx.NewMDBX(log.Root()).
			Path(dbPath).Label(label).
			DBVerbosity(config.DatabaseVerbosity).RoTxsLimiter(roTxsLimiter)
		if exclusive {
			opts = opts.Exclusive()
		}
		if label == kv.ChainDB {
			if config.MdbxPageSize.Bytes() > 0 {
				opts = opts.PageSize(config.MdbxPageSize.Bytes())
			}
			if config.MdbxDBSizeLimit > 0 {
				opts = opts.MapSize(config.MdbxDBSizeLimit)
			}
		} else {
			opts = opts.GrowthStep(16 * datasize.MB)
		}
		return opts.Open()
	}
	var err error
	db, err = openFunc(false)
	if err != nil {
		return nil, err
	}
	migrator := migrations.NewMigrator(label)
	if err := migrator.VerifyVersion(db); err != nil {
		return nil, err
	}

	has, err := migrator.HasPendingMigrations(db)
	if err != nil {
		return nil, err
	}
	if has {
		logger.Info("Re-Opening DB in exclusive mode to apply migrations")
		db.Close()
		db, err = openFunc(true)
		if err != nil {
			return nil, err
		}
		if err = migrator.Apply(db, config.Dirs.DataDir, logger); err != nil {
			return nil, err
		}
		db.Close()
		db, err = openFunc(false)
		if err != nil {
			return nil, err
		}
	}

	if err := db.Update(context.Background(), func(tx kv.RwTx) (err error) {
		return params.SetErigonVersion(tx, params.VersionKeyCreated)
	}); err != nil {
		return nil, err
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

	go debug.ListenSignals(stack)
}
