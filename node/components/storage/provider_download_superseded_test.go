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

package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

type recordingDeleter struct {
	mu    sync.Mutex
	calls []string
}

func (d *recordingDeleter) Delete(_ context.Context, paths []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, paths...)
	return nil
}

func (d *recordingDeleter) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.calls))
	copy(out, d.calls)
	return out
}

// The Provider's DownloadSuperseded subscriber forwards each event's
// file name to the downloader's Delete API. Fixes the pending-forever
// case where anacrolix retries a torrent no peer serves anymore.
func TestProvider_SubscribeDownloadSuperseded_CallsDelete(t *testing.T) {
	bus := event.NewEventBus(nil)
	p := &Provider{eventBus: bus, logger: log.Root()}
	dc := &recordingDeleter{}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	subscribeDownloadSupersededOn(ctx, p, dc)

	bus.Publish(flow.DownloadSuperseded{FileName: "v2.2-commitment.310-311.kv", Reason: "not-in-canonical"})

	require.Eventually(t, func() bool {
		return len(dc.snapshot()) == 1
	}, 2*time.Second, 10*time.Millisecond, "downloader.Delete called once")

	require.Equal(t, []string{"v2.2-commitment.310-311.kv"}, dc.snapshot())
}

// Empty FileName is a defensive no-op — don't waste a Delete call.
func TestProvider_SubscribeDownloadSuperseded_SkipsEmptyName(t *testing.T) {
	bus := event.NewEventBus(nil)
	p := &Provider{eventBus: bus, logger: log.Root()}
	dc := &recordingDeleter{}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	subscribeDownloadSupersededOn(ctx, p, dc)

	bus.Publish(flow.DownloadSuperseded{FileName: "", Reason: "not-in-canonical"})
	time.Sleep(100 * time.Millisecond)

	require.Empty(t, dc.snapshot(), "empty FileName must not trigger Delete")
}
