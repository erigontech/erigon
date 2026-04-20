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
type Transport interface {
	// Seed registers a file so remote peers sharing the Coordinator can
	// fetch it. Only metadata is passed (name + declared size) — the
	// simulated transport does not actually move bytes, so no payload is
	// needed. Real-transport implementations register a torrent.
	Seed(name string, size int64, infoHash [20]byte) error

	// Close stops any seeding + cancels in-flight downloads.
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

	mu      sync.Mutex
	closed  bool
	pending sync.WaitGroup
}

// NewSimulatedTransport wires a transport to the given bus and coordinator
// and subscribes it to DownloadRequested.
func NewSimulatedTransport(bus event.EventBus, coord *Coordinator) (*SimulatedTransport, error) {
	t := &SimulatedTransport{coord: coord, bus: bus}
	if err := bus.Subscribe(t.onDownloadRequested); err != nil {
		return nil, fmt.Errorf("subscribe DownloadRequested: %w", err)
	}
	return t, nil
}

func (t *SimulatedTransport) Seed(name string, size int64, infoHash [20]byte) error {
	t.coord.register(infoHash, name, size)
	return nil
}

// Close unsubscribes from the bus first so no new DownloadRequested handlers
// start, marks the transport closed, then waits for any in-flight simulated
// transfers to drain.
func (t *SimulatedTransport) Close() error {
	err := t.bus.Unsubscribe(t.onDownloadRequested)

	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()

	t.pending.Wait()
	return err
}

func (t *SimulatedTransport) onDownloadRequested(req flow.DownloadRequested) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.pending.Add(1)
	t.mu.Unlock()
	defer t.pending.Done()

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
