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

package downloader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/util"
	"github.com/erigontech/erigon/node/app/workerpool"
	"github.com/erigontech/erigon/node/components/storage/flow"
	downloaderproto "github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// fakeClient is a dl.Client implementation for unit tests. It records each
// Download call and returns downloadErr (if set) to let tests exercise both
// success and failure paths through the bus handler.
type fakeClient struct {
	mu           sync.Mutex
	downloadErr  error
	writeSize    int64
	rootDir      string
	downloadArgs []*downloaderproto.DownloadRequest
}

func (c *fakeClient) Download(_ context.Context, req *downloaderproto.DownloadRequest) error {
	c.mu.Lock()
	c.downloadArgs = append(c.downloadArgs, req)
	err := c.downloadErr
	writeSize := c.writeSize
	rootDir := c.rootDir
	c.mu.Unlock()

	if err != nil {
		return err
	}
	if writeSize > 0 {
		for _, item := range req.Items {
			target := filepath.Join(rootDir, item.Path)
			if mkErr := os.MkdirAll(filepath.Dir(target), 0o755); mkErr != nil {
				return mkErr
			}
			f, cErr := os.Create(target)
			if cErr != nil {
				return cErr
			}
			if tErr := f.Truncate(writeSize); tErr != nil {
				_ = f.Close()
				return tErr
			}
			if cErr := f.Close(); cErr != nil {
				return cErr
			}
		}
	}
	return nil
}

func (c *fakeClient) Seed(context.Context, []string) error   { return nil }
func (c *fakeClient) Delete(context.Context, []string) error { return nil }

func (c *fakeClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.downloadArgs)
}

var _ dl.Client = (*fakeClient)(nil)

type busTestEnv struct {
	bus     event.EventBus
	workers *workerpool.WorkerPool
	pool    *execPool
	client  *fakeClient
	p       *Provider
}

func (e *busTestEnv) close(t *testing.T) {
	t.Helper()
	_ = e.p.UnbindBus()
	e.bus.WaitAsync()
	e.pool.wg.Wait()
	e.workers.StopWait()
}

// execPool wraps workerpool.WorkerPool with a WaitGroup so tests can drain
// in-flight async handlers before asserting on published events.
type execPool struct {
	workers *workerpool.WorkerPool
	wg      sync.WaitGroup
}

func (p *execPool) Exec(task func()) {
	p.wg.Add(1)
	p.workers.Submit(func() {
		defer p.wg.Done()
		task()
	})
}

func (p *execPool) PoolSize() int  { return p.workers.Size() }
func (p *execPool) QueueSize() int { return p.workers.WaitingQueueSize() }

var _ util.ExecPool = (*execPool)(nil)

func newBusTestEnv(t *testing.T, client *fakeClient) *busTestEnv {
	t.Helper()
	workers := workerpool.New(4)
	pool := &execPool{workers: workers}
	bus := event.NewEventBus(pool)

	dirs := datadir.New(t.TempDir())
	client.rootDir = dirs.Snap

	p := &Provider{
		Client: client,
		dirs:   dirs,
		logger: log.Root(),
	}

	return &busTestEnv{
		bus:     bus,
		workers: workers,
		pool:    pool,
		client:  client,
		p:       p,
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

func TestBindBusDownloadComplete(t *testing.T) {
	env := newBusTestEnv(t, &fakeClient{writeSize: 1234})
	defer env.close(t)

	var completes []flow.DownloadComplete
	var mu sync.Mutex
	require.NoError(t, env.bus.Subscribe(func(e flow.DownloadComplete) {
		mu.Lock()
		completes = append(completes, e)
		mu.Unlock()
	}))

	require.NoError(t, env.p.BindBus(context.Background(), env.bus))

	req := flow.DownloadRequested{
		FileName: "v1.0-accounts.0-256.kv",
		InfoHash: [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}
	env.bus.Publish(req)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(completes) == 1
	}, 2*time.Second, "DownloadComplete publish")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, req.FileName, completes[0].FileName)
	require.Equal(t, req.InfoHash, completes[0].InfoHash)
	require.Equal(t, filepath.Join(env.p.dirs.Snap, req.FileName), completes[0].LocalPath)
	require.Equal(t, int64(1234), completes[0].Size)
	require.Equal(t, 1, env.client.callCount())
}

func TestBindBusDownloadFailed(t *testing.T) {
	env := newBusTestEnv(t, &fakeClient{downloadErr: errors.New("simulated failure")})
	defer env.close(t)

	var failures []flow.DownloadFailed
	var mu sync.Mutex
	require.NoError(t, env.bus.Subscribe(func(e flow.DownloadFailed) {
		mu.Lock()
		failures = append(failures, e)
		mu.Unlock()
	}))

	require.NoError(t, env.p.BindBus(context.Background(), env.bus))
	env.bus.Publish(flow.DownloadRequested{FileName: "broken.kv"})

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(failures) == 1
	}, 2*time.Second, "DownloadFailed publish")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "broken.kv", failures[0].FileName)
	require.Contains(t, failures[0].Reason, "simulated failure")
}

func TestBindBusRejectsNil(t *testing.T) {
	p := &Provider{}
	require.Error(t, p.BindBus(context.Background(), nil))
	require.Error(t, p.BindBus(context.Background(), event.NewEventBus(nil)))
}

func TestBindBusDoubleBind(t *testing.T) {
	env := newBusTestEnv(t, &fakeClient{})
	defer env.close(t)

	require.NoError(t, env.p.BindBus(context.Background(), env.bus))
	err := env.p.BindBus(context.Background(), env.bus)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already bound")
}

func TestUnbindBusIdempotent(t *testing.T) {
	env := newBusTestEnv(t, &fakeClient{})
	defer env.close(t)

	// Unbind before bind is a no-op
	require.NoError(t, env.p.UnbindBus())

	require.NoError(t, env.p.BindBus(context.Background(), env.bus))
	require.NoError(t, env.p.UnbindBus())
	require.NoError(t, env.p.UnbindBus())

	// After unbind, publishing should not trigger Download
	env.bus.Publish(flow.DownloadRequested{FileName: "after-unbind.kv"})
	env.bus.WaitAsync()
	env.pool.wg.Wait()
	require.Equal(t, 0, env.client.callCount())
}
