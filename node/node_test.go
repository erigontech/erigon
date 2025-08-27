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
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
)

var (
	testNodeKey, _ = crypto.GenerateKey()
)

func testNodeConfig(t *testing.T) *nodecfg.Config {
	return &nodecfg.Config{
		Name: "test node",
		P2P:  p2p.Config{PrivateKey: testNodeKey},
		Dirs: datadir.New(t.TempDir()),
	}
}

// Tests that an empty protocol stack can be closed more than once.
func TestNodeCloseMultipleTimes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	stack, err := New(context.Background(), testNodeConfig(t), log.New())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}
	stack.Close()

	// Ensure that a stopped node can be stopped again
	for i := 0; i < 3; i++ {
		if err := stack.Close(); !errors.Is(err, ErrNodeStopped) {
			t.Fatalf("iter %d: stop failure mismatch: have %v, want %v", i, err, ErrNodeStopped)
		}
	}
}

func TestNodeStartMultipleTimes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	stack, err := New(context.Background(), testNodeConfig(t), log.New())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}

	// Ensure that a node can be successfully started, but only once
	if err := stack.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	if err := stack.Start(); !errors.Is(err, ErrNodeRunning) {
		t.Fatalf("start failure mismatch: have %v, want %v ", err, ErrNodeRunning)
	}
	// Ensure that a node can be stopped, but only once
	if err := stack.Close(); err != nil {
		t.Fatalf("failed to stop node: %v", err)
	}
	if err := stack.Close(); !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("stop failure mismatch: have %v, want %v ", err, ErrNodeStopped)
	}
}

// Tests that if the data dir is already in use, an appropriate error is returned.
func TestNodeUsedDataDir(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	// Create a temporary folder to use as the data directory
	dir := t.TempDir()

	// Create a new node based on the data directory
	original, originalErr := New(context.Background(), &nodecfg.Config{Dirs: datadir.New(dir)}, log.New())
	if originalErr != nil {
		t.Fatalf("failed to create original protocol stack: %v", originalErr)
	}
	defer original.Close()
	if err := original.Start(); err != nil {
		t.Fatalf("failed to start original protocol stack: %v", err)
	}

	// Create a second node based on the same data directory and ensure failure
	if _, err := New(context.Background(), &nodecfg.Config{Dirs: datadir.New(dir)}, log.New()); !errors.Is(err, datadir.ErrDataDirLocked) {
		t.Fatalf("duplicate datadir failure mismatch: have %v, want %v", err, datadir.ErrDataDirLocked)
	}
}

// Tests whether a Lifecycle can be registered.
func TestLifecycleRegistry_Successful(t *testing.T) {
	stack, err := New(context.Background(), testNodeConfig(t), log.New())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}
	defer stack.Close()

	noop := NewNoop()
	stack.RegisterLifecycle(noop)

	if !containsLifecycle(stack.lifecycles, noop) {
		t.Fatalf("lifecycle was not properly registered on the node, %v", err)
	}
}

// Tests whether a service's protocols can be registered properly on the node's p2p server.
func TestRegisterProtocols(t *testing.T) {
	t.Skip("adjust to p2p sentry")
	// TODO
}

// This test checks that open databases are closed with node.
func TestNodeCloseClosesDB(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	logger := log.New()
	stack, _ := New(context.Background(), testNodeConfig(t), logger)
	defer stack.Close()

	db, err := OpenDatabase(context.Background(), stack.Config(), kv.SentryDB, "", false, logger)
	if err != nil {
		t.Fatal("can't open DB:", err)
	}
	if err = db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.Inodes, []byte("testK"), []byte{})
	}); err != nil {
		t.Fatal("can't Put on open DB:", err)
	}

	stack.Close()
	//if err = db.Update(context.Background(), func(tx kv.RwTx) error {
	//	return tx.Put(kv.Inodes, []byte("testK"), []byte{})
	//}); err == nil {
	//	t.Fatal("Put succeeded after node is closed")
	//}
}

// This test checks that OpenDatabase can be used from within a Lifecycle Start method.
func TestNodeOpenDatabaseFromLifecycleStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	logger := log.New()
	stack, err := New(context.Background(), testNodeConfig(t), logger)
	require.NoError(t, err)
	defer stack.Close()

	var db kv.RwDB
	stack.RegisterLifecycle(&InstrumentedService{
		startHook: func() {
			db, err = OpenDatabase(context.Background(), stack.Config(), kv.SentryDB, "", false, logger)
			if err != nil {
				t.Fatal("can't open DB:", err)
			}
		},
		stopHook: func() {
			db.Close()
		},
	})

	stack.Start() //nolint:errcheck
	stack.Close()
}

// This test checks that OpenDatabase can be used from within a Lifecycle Stop method.
func TestNodeOpenDatabaseFromLifecycleStop(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	logger := log.New()
	stack, _ := New(context.Background(), testNodeConfig(t), logger)
	defer stack.Close()

	stack.RegisterLifecycle(&InstrumentedService{
		stopHook: func() {
			db, err := OpenDatabase(context.Background(), stack.Config(), kv.ChainDB, "", false, logger)
			if err != nil {
				t.Fatal("can't open DB:", err)
			}
			db.Close()
		},
	})

	stack.Start() //nolint:errcheck
	stack.Close()
}

// Tests that registered Lifecycles get started and stopped correctly.
func TestLifecycleLifeCycle(t *testing.T) {
	stack, _ := New(context.Background(), testNodeConfig(t), log.New())
	defer stack.Close()

	started := make(map[string]bool)
	stopped := make(map[string]bool)

	// Create a batch of instrumented services
	lifecycles := map[string]Lifecycle{
		"A": &InstrumentedService{
			startHook: func() { started["A"] = true },
			stopHook:  func() { stopped["A"] = true },
		},
		"B": &InstrumentedService{
			startHook: func() { started["B"] = true },
			stopHook:  func() { stopped["B"] = true },
		},
		"C": &InstrumentedService{
			startHook: func() { started["C"] = true },
			stopHook:  func() { stopped["C"] = true },
		},
	}
	// register lifecycles on node
	for _, lifecycle := range lifecycles {
		stack.RegisterLifecycle(lifecycle)
	}
	// Start the node and check that all services are running
	if err := stack.Start(); err != nil {
		t.Fatalf("failed to start protocol stack: %v", err)
	}
	for id := range lifecycles {
		if !started[id] {
			t.Fatalf("service %s: freshly started service not running", id)
		}
		if stopped[id] {
			t.Fatalf("service %s: freshly started service already stopped", id)
		}
	}
	// Stop the node and check that all services have been stopped
	if err := stack.Close(); err != nil {
		t.Fatalf("failed to stop protocol stack: %v", err)
	}
	for id := range lifecycles {
		if !stopped[id] {
			t.Fatalf("service %s: freshly terminated service still running", id)
		}
	}
}

// Tests that if a Lifecycle fails to start, all others started before it will be
// shut down.
func TestLifecycleStartupError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	stack, err := New(context.Background(), testNodeConfig(t), log.New())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}
	defer stack.Close()

	started := make(map[string]bool)
	stopped := make(map[string]bool)

	// Create a batch of instrumented services
	lifecycles := map[string]Lifecycle{
		"A": &InstrumentedService{
			startHook: func() { started["A"] = true },
			stopHook:  func() { stopped["A"] = true },
		},
		"B": &InstrumentedService{
			startHook: func() { started["B"] = true },
			stopHook:  func() { stopped["B"] = true },
		},
		"C": &InstrumentedService{
			startHook: func() { started["C"] = true },
			stopHook:  func() { stopped["C"] = true },
		},
	}
	// register lifecycles on node
	for _, lifecycle := range lifecycles {
		stack.RegisterLifecycle(lifecycle)
	}

	// Register a service that fails to construct itself
	failure := errors.New("fail")
	failer := &InstrumentedService{start: failure}
	stack.RegisterLifecycle(failer)

	// Start the protocol stack and ensure all started services stop
	if err := stack.Start(); !errors.Is(err, failure) {
		t.Fatalf("stack startup failure mismatch: have %v, want %v", err, failure)
	}
	for id := range lifecycles {
		if started[id] && !stopped[id] {
			t.Fatalf("service %s: started but not stopped", id)
		}
		delete(started, id)
		delete(stopped, id)
	}
}

// Tests that even if a registered Lifecycle fails to shut down cleanly, it does
// not influence the rest of the shutdown invocations.
func TestLifecycleTerminationGuarantee(t *testing.T) {
	stack, err := New(context.Background(), testNodeConfig(t), log.New())
	if err != nil {
		t.Fatalf("failed to create protocol stack: %v", err)
	}
	defer stack.Close()

	started := make(map[string]bool)
	stopped := make(map[string]bool)

	// Create a batch of instrumented services
	lifecycles := map[string]Lifecycle{
		"A": &InstrumentedService{
			startHook: func() { started["A"] = true },
			stopHook:  func() { stopped["A"] = true },
		},
		"B": &InstrumentedService{
			startHook: func() { started["B"] = true },
			stopHook:  func() { stopped["B"] = true },
		},
		"C": &InstrumentedService{
			startHook: func() { started["C"] = true },
			stopHook:  func() { stopped["C"] = true },
		},
	}
	// register lifecycles on node
	for _, lifecycle := range lifecycles {
		stack.RegisterLifecycle(lifecycle)
	}

	// Register a service that fails to shot down cleanly
	failure := errors.New("fail")
	failer := &InstrumentedService{stop: failure}
	stack.RegisterLifecycle(failer)

	// Start the protocol stack, and ensure that a failing shut down terminates all
	// Start the stack and make sure all is online
	if err1 := stack.Start(); err != nil {
		t.Fatalf("failed to start protocol stack: %v", err1)
	}
	for id := range lifecycles {
		if !started[id] {
			t.Fatalf("service %s: service not running", id)
		}
		if stopped[id] {
			t.Fatalf("service %s: service already stopped", id)
		}
	}
	// Stop the stack, verify failure and check all terminations
	err = stack.Close()
	if err, ok := err.(*StopError); !ok {
		t.Fatalf("termination failure mismatch: have %v, want StopError", err)
	} else {
		failer := reflect.TypeOf(&InstrumentedService{})
		if !errors.Is(err.Services[failer], failure) {
			t.Fatalf("failer termination failure mismatch: have %v, want %v", err.Services[failer], failure)
		}
		if len(err.Services) != 1 {
			t.Fatalf("failure count mismatch: have %d, want %d", len(err.Services), 1)
		}
	}
	for id := range lifecycles {
		if !stopped[id] {
			t.Fatalf("service %s: service not terminated", id)
		}
		delete(started, id)
		delete(stopped, id)
	}
}

func containsProtocol(stackProtocols []p2p.Protocol, protocol p2p.Protocol) bool {
	for _, a := range stackProtocols {
		if reflect.DeepEqual(a, protocol) {
			return true
		}
	}
	return false
}
