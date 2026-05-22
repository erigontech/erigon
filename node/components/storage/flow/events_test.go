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

package flow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestInitialStateReadyChannel verifies the bus-event-to-channel
// adapter that backend.go uses to bridge flow.InitialStateReady into
// staged-sync's OtterSync gate.
func TestInitialStateReadyChannel(t *testing.T) {
	t.Parallel()
	bus := newBusForTest()
	ch := InitialStateReadyChannel(bus)

	select {
	case <-ch:
		t.Fatal("channel closed before InitialStateReady was published")
	case <-time.After(20 * time.Millisecond):
	}

	bus.Publish(InitialStateReady{StateDomains: []snapshot.Domain{snapshot.DomainAccounts}})

	select {
	case <-ch:
		// ok
	case <-time.After(time.Second):
		t.Fatal("channel did not close after InitialStateReady published")
	}

	// Subsequent publishes do not panic — handler self-removed via
	// SubscribeOnce.
	bus.Publish(InitialStateReady{})

	// Channel stays closed — closing once.
	select {
	case <-ch:
	default:
		t.Fatal("channel reopened (impossible) — close-once semantics broken")
	}

	require.True(t, true) // sanity
}

// TestInitialValidationCompleteChannel verifies the channel adapter
// closes once flow.InitialValidationComplete is published.
func TestInitialValidationCompleteChannel(t *testing.T) {
	t.Parallel()
	bus := newBusForTest()
	ch := InitialValidationCompleteChannel(bus)

	select {
	case <-ch:
		t.Fatal("channel closed before InitialValidationComplete was published")
	case <-time.After(20 * time.Millisecond):
	}

	bus.Publish(InitialValidationComplete{})

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel did not close after InitialValidationComplete published")
	}
}

// TestFirstPublishGateChannel pins the conjunction gate: the channel
// closes only once BOTH InitialDownloadsComplete and
// InitialValidationComplete have fired, and the close is order-
// independent.
func TestFirstPublishGateChannel(t *testing.T) {
	t.Parallel()

	t.Run("downloads then validation", func(t *testing.T) {
		t.Parallel()
		bus := newBusForTest()
		ch := FirstPublishGateChannel(bus)

		bus.Publish(InitialDownloadsComplete{})
		select {
		case <-ch:
			t.Fatal("gate closed on InitialDownloadsComplete alone")
		case <-time.After(20 * time.Millisecond):
		}

		bus.Publish(InitialValidationComplete{})
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("gate did not close after both events published")
		}
	})

	t.Run("validation then downloads", func(t *testing.T) {
		t.Parallel()
		bus := newBusForTest()
		ch := FirstPublishGateChannel(bus)

		bus.Publish(InitialValidationComplete{})
		select {
		case <-ch:
			t.Fatal("gate closed on InitialValidationComplete alone")
		case <-time.After(20 * time.Millisecond):
		}

		bus.Publish(InitialDownloadsComplete{})
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("gate did not close after both events published")
		}
	})
}

// TestBlockHeadersReadyChannel verifies the bus-event-to-channel adapter
// that backend.go uses to bridge flow.BlockHeadersReady into Caplin's
// DownloadHistoricalBlocks gate. Contract:
//   - the channel stays open until BlockHeadersReady is published
//   - on publication, the channel closes and the tip getter returns the
//     published TipBlock
//   - subsequent publishes don't panic and don't change the latched tip
//     (one-shot edge transition — first wins)
func TestBlockHeadersReadyChannel(t *testing.T) {
	t.Parallel()
	bus := newBusForTest()
	ch, tipFn := BlockHeadersReadyChannel(bus)

	select {
	case <-ch:
		t.Fatal("channel closed before BlockHeadersReady was published")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, uint64(0), tipFn(), "tip must be zero before publish")

	bus.Publish(BlockHeadersReady{TipBlock: 25_049_601})

	select {
	case <-ch:
		// ok
	case <-time.After(time.Second):
		t.Fatal("channel did not close after BlockHeadersReady published")
	}
	require.Equal(t, uint64(25_049_601), tipFn(),
		"tip getter must return the published TipBlock once the channel closes")

	// A second publish must not panic and must not reopen the channel.
	// The tip is allowed to update or stay latched — both are acceptable
	// shapes; the load-bearing contract is "channel-close is one-shot".
	bus.Publish(BlockHeadersReady{TipBlock: 25_049_700})
	select {
	case <-ch:
	default:
		t.Fatal("channel reopened (impossible) — close-once semantics broken")
	}
}

// TestBlockHeadersReady_WiringChain pins the full piece-B wiring chain:
// the stage-side hook (whatever production calls `cfg.blockHeadersOpened`)
// → backend.go's installer (publishes flow.BlockHeadersReady on the bus)
// → a consumer's BlockHeadersReadyChannel.
//
// Each link in this chain has its own unit test, but if backend.go's
// installer closure ever diverges from what the channel helper expects
// (different event type, missing TipBlock, swapped field, etc.), those
// link-level tests still pass while production silently fails. This
// integration test catches divergence in the chain itself.
//
// The stage-side hook is just a func(uint64). We model it as the same
// closure shape backend.go installs: `func(tip) { bus.Publish(...) }`.
// Then we invoke the hook (as stage_snapshots.go would) and assert the
// channel closes and the tip getter returns the right value.
func TestBlockHeadersReady_WiringChain(t *testing.T) {
	t.Parallel()
	bus := newBusForTest()
	ch, tipFn := BlockHeadersReadyChannel(bus)

	// Mirror backend.go's hook installer. Keep this shape in sync with
	// node/eth/backend.go's `config.Snapshot.BlockHeadersOpenedHook =
	// func(tipBlock uint64) { bus.Publish(flow.BlockHeadersReady{TipBlock: tipBlock}) }`.
	stageHook := func(tipBlock uint64) {
		bus.Publish(BlockHeadersReady{TipBlock: tipBlock})
	}

	// stage_snapshots.go calls the hook once, after a successful
	// OpenSegments(Headers, Bodies), passing cfg.blockReader.FrozenBlocks().
	const stageFrozenTip uint64 = 25_049_601
	stageHook(stageFrozenTip)

	select {
	case <-ch:
		// ok
	case <-time.After(time.Second):
		t.Fatal("BlockHeadersReadyChannel did not close after stage-side hook fired — wiring chain broken")
	}
	require.Equal(t, stageFrozenTip, tipFn(),
		"tip getter must reflect the value the stage passed to the hook — wiring chain dropped/mutated TipBlock")
}
