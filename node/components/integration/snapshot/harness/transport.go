// Copyright 2026 The Erigon Authors
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

package harness

import (
	"fmt"
	"sync"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

// Transport abstracts the file exchange layer between peers. Production nodes
// use an anacrolix/torrent client; harness scenarios use SimulatedTransport
// (in-process event exchange through a shared Coordinator).
//
// A Transport subscribes to DownloadRequested on its bus and responds by
// publishing DownloadComplete (success) or DownloadFailed (e.g. unknown
// info-hash).
//
// Seeding-side surface is intentionally absent from this interface. FakePeer
// registers with the Coordinator directly; real-transport implementations
// (anacrolix/torrent) will own their own seeding lifecycle as part of their
// construction, not via a Transport-level method.
type Transport interface {
	// Close stops the transport and cancels in-flight downloads.
	Close() error
}

// Coordinator is the shared registry used by SimulatedTransport instances to
// find each other's seeded files. Production transport replaces this with a
// DHT and a torrent swarm.
type Coordinator struct {
	mu    sync.RWMutex
	files map[[20]byte]fileData
}

type fileData struct {
	name string
	size int64
}

// NewCoordinator returns an empty coordinator shared by a set of simulated
// transports in a test scenario.
func NewCoordinator() *Coordinator {
	return &Coordinator{files: make(map[[20]byte]fileData)}
}

func (c *Coordinator) register(infoHash [20]byte, name string, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files[infoHash] = fileData{name: name, size: size}
}

func (c *Coordinator) lookup(infoHash [20]byte) (fileData, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	fd, ok := c.files[infoHash]
	return fd, ok
}

// SimulatedTransport copies bytes through a shared Coordinator. It subscribes
// to DownloadRequested on its bus; on request it looks up the info-hash in
// the Coordinator, and publishes DownloadComplete (with a synthetic local
// path) or DownloadFailed.
//
// No disk I/O: integration tests assert against event flow and inventory
// state, not on-disk artefacts. Switch to a real-bytes variant when M3+
// needs it.
type SimulatedTransport struct {
	coord *Coordinator
	bus   event.EventBus

	// handler is the method value stored once at construction so Subscribe
	// and Unsubscribe see the same reflect.Value.Pointer(). Re-referencing
	// the method (t.onDownloadRequested) allocates a fresh closure whose
	// pointer the event bus cannot reliably match against the one it
	// recorded at Subscribe time.
	handler func(flow.DownloadRequested)

	mu      sync.Mutex
	closed  bool
	failFn  func(name string) bool
	pending sync.WaitGroup
}

// SetFailFn installs a predicate that decides whether to fail a given
// DownloadRequested by file name. When the predicate returns true, the
// transport publishes DownloadFailed instead of DownloadComplete. Used by
// soak tests to inject intermittent failures.
//
// Passing nil removes the predicate.
func (t *SimulatedTransport) SetFailFn(fn func(name string) bool) {
	t.mu.Lock()
	t.failFn = fn
	t.mu.Unlock()
}

// NewSimulatedTransport wires a transport to the given bus and coordinator
// and subscribes it to DownloadRequested.
func NewSimulatedTransport(bus event.EventBus, coord *Coordinator) (*SimulatedTransport, error) {
	t := &SimulatedTransport{coord: coord, bus: bus}
	t.handler = t.onDownloadRequested
	if err := bus.Subscribe(t.handler); err != nil {
		return nil, fmt.Errorf("subscribe DownloadRequested: %w", err)
	}
	return t, nil
}

// Close unsubscribes from the bus first so no new DownloadRequested handlers
// start, marks the transport closed, then waits for any in-flight simulated
// transfers to drain. Idempotent: repeated Close calls return nil without
// touching the bus.
func (t *SimulatedTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()

	err := t.bus.Unsubscribe(t.handler)
	t.pending.Wait()
	return err
}

func (t *SimulatedTransport) onDownloadRequested(req flow.DownloadRequested) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	failFn := t.failFn
	t.pending.Add(1)
	t.mu.Unlock()
	defer t.pending.Done()

	if failFn != nil && failFn(req.FileName) {
		t.bus.Publish(flow.DownloadFailed{
			FileName: req.FileName,
			Reason:   "fault injection",
		})
		return
	}

	fd, ok := t.coord.lookup(req.InfoHash)
	if !ok {
		t.bus.Publish(flow.DownloadFailed{
			FileName: req.FileName,
			Reason:   "info-hash not registered with coordinator",
		})
		return
	}
	t.bus.Publish(flow.DownloadComplete{
		FileName:  req.FileName,
		InfoHash:  req.InfoHash,
		LocalPath: "/sim/" + fd.name,
		Size:      fd.size,
	})
}
