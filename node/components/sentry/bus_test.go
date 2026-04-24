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

package sentry

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func newTestBus() event.EventBus { return event.NewEventBus(nil) }

func TestSentryBindBusRejectsNil(t *testing.T) {
	p := &Provider{}
	require.Error(t, p.BindBus(nil))
}

func TestSentryBindBusDoubleBind(t *testing.T) {
	p := &Provider{}
	bus := newTestBus()
	require.NoError(t, p.BindBus(bus))
	err := p.BindBus(bus)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already bound")
}

func TestAnnouncePeerManifestPublishes(t *testing.T) {
	p := &Provider{}
	bus := newTestBus()
	require.NoError(t, p.BindBus(bus))

	var got []flow.PeerManifestReceived
	var mu sync.Mutex
	require.NoError(t, bus.Subscribe(func(e flow.PeerManifestReceived) {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
	}))

	domains := map[snapshot.Domain][]*snapshot.FileEntry{
		snapshot.Domain("accounts"): {{Name: "v1.0-accounts.0-256.kv", FromStep: 0, ToStep: 256}},
	}
	blocks := []*snapshot.FileEntry{{Name: "v1.0-000000-000500-headers.seg"}}

	p.AnnouncePeerManifest("peer-A", domains, blocks)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1)
	require.Equal(t, "peer-A", got[0].PeerID)
	require.Equal(t, domains, got[0].Domains)
	require.Equal(t, blocks, got[0].Blocks)
}

func TestAnnouncePeerDepartedPublishes(t *testing.T) {
	p := &Provider{}
	bus := newTestBus()
	require.NoError(t, p.BindBus(bus))

	var gotDep []flow.PeerDeparted
	var mu sync.Mutex
	require.NoError(t, bus.Subscribe(func(e flow.PeerDeparted) {
		mu.Lock()
		gotDep = append(gotDep, e)
		mu.Unlock()
	}))

	// PeerDeparted for an unannounced peer is a no-op.
	p.AnnouncePeerDeparted("never-announced")
	mu.Lock()
	require.Empty(t, gotDep)
	mu.Unlock()

	// Announce then depart.
	p.AnnouncePeerManifest("peer-B", nil, nil)
	p.AnnouncePeerDeparted("peer-B")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, gotDep, 1)
	require.Equal(t, "peer-B", gotDep[0].PeerID)
}

func TestUnbindBusPublishesDepartedForRemainingPeers(t *testing.T) {
	p := &Provider{}
	bus := newTestBus()
	require.NoError(t, p.BindBus(bus))

	var gotDep []flow.PeerDeparted
	var mu sync.Mutex
	require.NoError(t, bus.Subscribe(func(e flow.PeerDeparted) {
		mu.Lock()
		gotDep = append(gotDep, e)
		mu.Unlock()
	}))

	p.AnnouncePeerManifest("peer-X", nil, nil)
	p.AnnouncePeerManifest("peer-Y", nil, nil)
	p.AnnouncePeerManifest("peer-Z", nil, nil)

	// Explicit depart for one peer; the other two should be published by UnbindBus.
	p.AnnouncePeerDeparted("peer-Y")
	require.NoError(t, p.UnbindBus())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, gotDep, 3)
	ids := []string{gotDep[0].PeerID, gotDep[1].PeerID, gotDep[2].PeerID}
	require.Contains(t, ids, "peer-X")
	require.Contains(t, ids, "peer-Y")
	require.Contains(t, ids, "peer-Z")
}

func TestAnnounceBeforeBindIsNoop(t *testing.T) {
	p := &Provider{}
	// No BindBus — should not panic, should not publish anywhere.
	p.AnnouncePeerManifest("peer-ghost", nil, nil)
	p.AnnouncePeerDeparted("peer-ghost")
	require.NoError(t, p.UnbindBus())
}
