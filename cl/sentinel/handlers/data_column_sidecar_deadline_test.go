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

package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/db/kv"
)

var errTestWriteDeadline = errors.New("write deadline unavailable")

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

// columnResponseStream records when the handler renews the write deadline relative to the response bytes.
type columnResponseStream struct {
	envelopeHandlerStream
	beforeFirstRead      func()
	writeDeadlines       []time.Time
	bytesAtWriteDeadline []int
	failWriteDeadlineAt  int
	reset                bool
	closed               bool
}

func (s *columnResponseStream) Read(p []byte) (int, error) {
	if s.beforeFirstRead != nil {
		s.beforeFirstRead()
		s.beforeFirstRead = nil
	}
	return s.envelopeHandlerStream.Read(p)
}

func (s *columnResponseStream) SetDeadline(time.Time) error { return nil }
func (s *columnResponseStream) Reset() error                { s.reset = true; return nil }
func (s *columnResponseStream) Close() error                { s.closed = true; return nil }

func (s *columnResponseStream) SetWriteDeadline(t time.Time) error {
	s.writeDeadlines = append(s.writeDeadlines, t)
	s.bytesAtWriteDeadline = append(s.bytesAtWriteDeadline, s.response.Len())
	if len(s.writeDeadlines) == s.failWriteDeadlineAt {
		return errTestWriteDeadline
	}
	return nil
}

// clockedColumnStore stores the columns accepted by stored (all when nil) and advances the clock by step for each streamed sidecar.
type clockedColumnStore struct {
	blob_storage.DataColumnStorage
	clock   *fakeClock
	step    time.Duration
	stored  func(column int64) bool
	probes  int
	onProbe func(probe int)
}

func (s *clockedColumnStore) ColumnSidecarExists(_ context.Context, _ uint64, _ common.Hash, column int64) (bool, error) {
	s.probes++
	if s.onProbe != nil {
		s.onProbe(s.probes)
	}
	return s.stored == nil || s.stored(column), nil
}

func (s *clockedColumnStore) WriteStream(w io.Writer, _ uint64, _ common.Hash, column uint64) error {
	s.clock.advance(s.step)
	sidecar := cltypes.NewDataColumnSidecar()
	sidecar.Index = column
	return ssz_snappy.EncodeAndWrite(w, sidecar)
}

func decodeColumnChunks(t *testing.T, response []byte) (columns []uint64, chunkEnds []int) {
	t.Helper()
	r := bytes.NewReader(response)
	for r.Len() > 0 {
		prefix, err := r.ReadByte()
		require.NoError(t, err)
		require.Equal(t, byte(SuccessfulResponsePrefix), prefix)
		_, err = io.ReadFull(r, make([]byte, 4))
		require.NoError(t, err)
		sidecar := cltypes.NewDataColumnSidecar()
		require.NoError(t, ssz_snappy.DecodeAndReadNoForkDigest(r, sidecar, clparams.FuluVersion))
		columns = append(columns, sidecar.Index)
		chunkEnds = append(chunkEnds, len(response)-r.Len())
	}
	return columns, chunkEnds
}

type columnHandlerFixture struct {
	handlers *ConsensusHandlers
	db       kv.RwDB
	clock    *fakeClock
	store    *clockedColumnStore
	stream   *columnResponseStream
	columns  solid.Uint64ListSSZ
}

// newColumnHandlerFixture requests columns 0..columns-1 for every block; the fake store reports all of them as stored.
// The fake clock moves by step for each streamed sidecar and by whatever a test sets in the stream's beforeFirstRead hook.
func newColumnHandlerFixture(t *testing.T, step time.Duration, columns int) *columnHandlerFixture {
	t.Helper()
	_, mainnetCfg := clparams.GetConfigsByNetwork(1)
	initDataColumnSidecarTestConfig.Do(func() {
		if clparams.GetBeaconConfig() == nil {
			clparams.InitGlobalStaticConfig(mainnetCfg)
		}
	})
	cfg := *mainnetCfg
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0, 0, 0, 0
	cfg.InitializeForkSchedule()
	_, indiciesDB := setupStore(t)

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	store := &clockedColumnStore{clock: clock, step: step}
	requested := solid.NewUint64ListSSZ(int(cfg.NumberOfColumns))
	for column := range columns {
		requested.Append(uint64(column))
	}
	return &columnHandlerFixture{
		handlers: &ConsensusHandlers{
			ctx:               t.Context(),
			indiciesDB:        indiciesDB,
			beaconConfig:      &cfg,
			dataColumnStorage: store,
			ethClock:          eth_clock.NewEthereumClock(uint64(time.Now().Add(-time.Hour).Unix()), common.Hash{}, &cfg),
			rateLimiter:       newPeerRateLimiter(),
			now:               clock.now,
		},
		db:      indiciesDB,
		clock:   clock,
		store:   store,
		stream:  &columnResponseStream{envelopeHandlerStream: envelopeHandlerStream{conn: &remotePeerConn{peerID: peer.ID("column-peer")}}},
		columns: requested,
	}
}

// prepareByRoot queues a request for the fixture's columns of one stored block root.
func (f *columnHandlerFixture) prepareByRoot(t *testing.T) {
	t.Helper()
	root := common.Hash{0x42}
	tx, err := f.db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, 0))
	require.NoError(t, tx.Commit())

	byRoot := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	byRoot.Append(&cltypes.DataColumnsByRootIdentifier{BlockRoot: root, Columns: f.columns})
	f.queueRequest(t, byRoot, communication.DataColumnSidecarsByRootProtocolV1)
}

func (f *columnHandlerFixture) serveByRoot(t *testing.T) error {
	t.Helper()
	f.prepareByRoot(t)
	return f.handlers.dataColumnSidecarsByRootHandler(f.stream)
}

// serveByRange requests the fixture's columns of two stored blocks through the ByRange handler.
func (f *columnHandlerFixture) serveByRange(t *testing.T) error {
	t.Helper()
	tx, err := f.db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for slot := range uint64(2) {
		require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, common.Hash{byte(slot + 1)}))
	}
	require.NoError(t, tx.Commit())

	f.queueRequest(t, &cltypes.ColumnSidecarsByRangeRequest{Count: 2, Columns: f.columns}, communication.DataColumnSidecarsByRangeProtocolV1)
	return f.handlers.dataColumnSidecarsByRangeHandler(f.stream)
}

func (f *columnHandlerFixture) queueRequest(t *testing.T, request ssz.EncodableSSZ, proto string) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&buf, request))
	f.stream.request = bytes.NewReader(buf.Bytes())
	f.stream.protocol = protocol.ID(proto)
}

func TestDataColumnSidecarsByRootRenewsWriteDeadlineBeforeEverySidecar(t *testing.T) {
	f := newColumnHandlerFixture(t, time.Second, 5)
	start := f.clock.now()

	require.NoError(t, f.serveByRoot(t))

	columns, chunkEnds := decodeColumnChunks(t, f.stream.response.Bytes())
	require.Equal(t, []uint64{0, 1, 2, 3, 4}, columns)
	require.Equal(t, []int{0, chunkEnds[0], chunkEnds[1], chunkEnds[2], chunkEnds[3]}, f.stream.bytesAtWriteDeadline)
	var want []time.Time
	for i := range 5 {
		want = append(want, start.Add(time.Duration(i)*time.Second+10*time.Second))
	}
	require.Equal(t, want, f.stream.writeDeadlines)
}

func TestDataColumnSidecarsByRangeRenewsWriteDeadlineBeforeEverySidecar(t *testing.T) {
	f := newColumnHandlerFixture(t, time.Second, 3)
	start := f.clock.now()

	require.NoError(t, f.serveByRange(t))

	columns, chunkEnds := decodeColumnChunks(t, f.stream.response.Bytes())
	require.Equal(t, []uint64{0, 1, 2, 0, 1, 2}, columns)
	require.Equal(t, []int{0, chunkEnds[0], chunkEnds[1], chunkEnds[2], chunkEnds[3], chunkEnds[4]}, f.stream.bytesAtWriteDeadline)
	var want []time.Time
	for i := range 6 {
		want = append(want, start.Add(time.Duration(i)*time.Second+10*time.Second))
	}
	require.Equal(t, want, f.stream.writeDeadlines)
}

func TestDataColumnSidecarsByRootStopsWhenResponseBudgetIsSpent(t *testing.T) {
	for _, tc := range []struct {
		name        string
		readAdvance time.Duration
		step        time.Duration
		wantServed  int
	}{
		{"checked between sidecars", 0, 12 * time.Second, 3},
		{"elapsed exactly the budget between sidecars", 20 * time.Second, 10 * time.Second, 1},
		{"elapsed just below the budget between sidecars", 20 * time.Second, 10*time.Second - time.Nanosecond, 2},
		{"spent before the first sidecar still serves one", 30 * time.Second, 0, 1},
		{"just below the budget before the first sidecar serves all", 30*time.Second - time.Nanosecond, 0, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newColumnHandlerFixture(t, tc.step, 5)
			f.stream.beforeFirstRead = func() { f.clock.advance(tc.readAdvance) }

			require.NoError(t, f.serveByRoot(t))

			columns, _ := decodeColumnChunks(t, f.stream.response.Bytes())
			require.Len(t, columns, tc.wantServed)
		})
	}
}

func TestDataColumnSidecarsWriteDeadlineFailureStopsBeforeTheChunk(t *testing.T) {
	for _, handler := range []struct {
		name    string
		columns int
		serve   func(f *columnHandlerFixture, t *testing.T) error
	}{
		{"by root", 5, (*columnHandlerFixture).serveByRoot},
		{"by range", 3, (*columnHandlerFixture).serveByRange},
	} {
		for _, failAt := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s renewal %d", handler.name, failAt), func(t *testing.T) {
				f := newColumnHandlerFixture(t, time.Second, handler.columns)
				f.stream.failWriteDeadlineAt = failAt

				err := handler.serve(f, t)

				require.ErrorIs(t, err, errTestWriteDeadline)
				columns, _ := decodeColumnChunks(t, f.stream.response.Bytes())
				require.Len(t, columns, failAt-1)
				require.Len(t, f.stream.writeDeadlines, failAt)
			})
		}
	}
}

func TestDataColumnSidecarsByRootStopsProbingOnceBudgetIsSpent(t *testing.T) {
	f := newColumnHandlerFixture(t, 30*time.Second, 5)
	f.store.stored = func(column int64) bool { return column == 0 }

	require.NoError(t, f.serveByRoot(t))

	columns, _ := decodeColumnChunks(t, f.stream.response.Bytes())
	require.Equal(t, []uint64{0}, columns)
	require.Equal(t, 1, f.store.probes, "no column may be probed after the budget is spent")
}

func TestDataColumnSidecarsByRootStopsWhenProbeSpendsBudget(t *testing.T) {
	f := newColumnHandlerFixture(t, 0, 5)
	f.store.onProbe = func(probe int) {
		if probe == 2 {
			f.clock.advance(columnByRootResponseBudget)
		}
	}

	require.NoError(t, f.serveByRoot(t))

	columns, _ := decodeColumnChunks(t, f.stream.response.Bytes())
	require.Equal(t, []uint64{0}, columns)
	require.Equal(t, 2, f.store.probes)
	require.Len(t, f.stream.writeDeadlines, 1)
}

func TestStreamHandlerClosesNormallyWhenColumnResponseBudgetIsSpent(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	f := newColumnHandlerFixture(t, 30*time.Second, 5)
	f.handlers.host = h
	f.prepareByRoot(t)

	f.handlers.wrapStreamHandler(communication.DataColumnSidecarsByRootProtocolV1, f.handlers.dataColumnSidecarsByRootHandler)(f.stream)

	columns, _ := decodeColumnChunks(t, f.stream.response.Bytes())
	require.Len(t, columns, 1)
	require.True(t, f.stream.closed)
	require.False(t, f.stream.reset)
}

func TestDataColumnSidecarsRenewedWriteDeadlineIsCappedAtResponseHardStop(t *testing.T) {
	for _, tc := range []struct {
		name        string
		readAdvance time.Duration
		serve       func(f *columnHandlerFixture, t *testing.T) error
	}{
		{"by root with the request read spending the budget", 35 * time.Second, (*columnHandlerFixture).serveByRoot},
		{"by range", 0, (*columnHandlerFixture).serveByRange},
		{"by range with the request read spending the budget", 35 * time.Second, (*columnHandlerFixture).serveByRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newColumnHandlerFixture(t, 9*time.Second, 128)
			f.stream.beforeFirstRead = func() { f.clock.advance(tc.readAdvance) }
			hardStop := f.clock.now().Add(40 * time.Second)

			require.NoError(t, tc.serve(f, t))

			require.NotEmpty(t, f.stream.writeDeadlines)
			for _, deadline := range f.stream.writeDeadlines {
				require.Falsef(t, deadline.After(hardStop), "write deadline %v is after the hard stop %v", deadline, hardStop)
			}
			require.Equal(t, hardStop, f.stream.writeDeadlines[len(f.stream.writeDeadlines)-1])
		})
	}
}
