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

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	mock_services2 "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type contentsEventKind string

const (
	eventColumnPublish       contentsEventKind = "column publish"
	eventColumnWrite         contentsEventKind = "column write"
	eventEnvelopePublish     contentsEventKind = "envelope publish"
	eventConsensusValidation contentsEventKind = "consensus validation"
	eventImport              contentsEventKind = "import"
)

// contentsEvent is one entry of the ordered log of what the handler does.
type contentsEvent struct {
	kind       contentsEventKind
	index      uint64
	err        error
	ctxErr     error
	forkDigest common.Bytes4
	deadline   time.Time
	data       []byte
}

// envelopeContentsFixture posts a Gloas envelope whose block has a bid with real KZG blobs to a
// handler with a real column store and a real custody-8 PeerDas, and logs what the handler does.
type envelopeContentsFixture struct {
	t                         *testing.T
	handler                   *ApiHandler
	forkchoice                *mock_services2.ForkChoiceStorageMock
	blobs                     []testBlob
	slot                      uint64
	currentSlot               uint64
	root                      common.Hash
	signedEnvelope            *cltypes.SignedExecutionPayloadEnvelope
	columnEvents              chan *beaconevents.EventStream
	digestEpochs              []uint64
	events                    []contentsEvent
	duringConsensusValidation func()
	consensusCheckBlobData    bool
	payloadInvalidated        bool
	invalidatedBlockRoot      common.Hash
	invalidatedExecutionHash  common.Hash

	failWrite    func(index uint64) error
	failPublish  func(index uint64) error
	failEnvelope error
	onClaim      func()
}

type loggingColumnStorage struct {
	blob_storage.DataColumnStorage
	f *envelopeContentsFixture
}

func (s *loggingColumnStorage) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, sidecar *cltypes.DataColumnSidecar) error {
	var err error
	if s.f.failWrite != nil {
		err = s.f.failWrite(uint64(columnIndex))
	}
	if err == nil {
		err = s.DataColumnStorage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	}
	deadline, _ := ctx.Deadline()
	s.f.record(contentsEvent{kind: eventColumnWrite, index: uint64(columnIndex), err: err, ctxErr: ctx.Err(), deadline: deadline})
	return err
}

// dataAvailabilityForkchoice makes the consensus-mode validation run the same data availability
// check that the real fork choice runs.
type dataAvailabilityForkchoice struct {
	*mock_services2.ForkChoiceStorageMock
	f *envelopeContentsFixture
}

func (d *dataAvailabilityForkchoice) ValidateExecutionPayloadEnvelopeForConsensus(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData bool) error {
	d.f.record(contentsEvent{kind: eventConsensusValidation})
	d.f.consensusCheckBlobData = checkBlobData
	if d.f.duringConsensusValidation != nil {
		d.f.duringConsensusValidation()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	finalizedSlot := d.FinalizedCheckpointVal.Epoch * d.f.handler.beaconChainCfg.SlotsPerEpoch
	if envelope.Message.Payload.SlotNumber < finalizedSlot {
		return fmt.Errorf("envelope slot %d is before finalized slot %d", envelope.Message.Payload.SlotNumber, finalizedSlot)
	}
	if checkBlobData && !d.f.dataAvailable() {
		return forkchoice.ErrEIP7594ColumnDataNotAvailable
	}
	return d.ForkChoiceStorageMock.ValidateExecutionPayloadEnvelopeForConsensus(ctx, envelope, checkBlobData)
}

func (d *dataAvailabilityForkchoice) ExecutionPayloadInvalidated(blockRoot, executionBlockHash common.Hash) bool {
	d.f.invalidatedBlockRoot = blockRoot
	d.f.invalidatedExecutionHash = executionBlockHash
	return d.f.payloadInvalidated
}

func (d *dataAvailabilityForkchoice) ClaimExecutionPayloadEnvelopeForGossip(ctx context.Context, root common.Hash, builderIndex uint64) (forkchoice.ExecutionPayloadEnvelopeAdmissionToken, error) {
	if d.f.onClaim != nil {
		d.f.onClaim()
	}
	return d.ForkChoiceStorageMock.ClaimExecutionPayloadEnvelopeForGossip(ctx, root, builderIndex)
}

var wallClockForkDigest = common.Bytes4{0xff, 0xff, 0xff, 0xff}

func forkDigestOfEpoch(epoch uint64) common.Bytes4 {
	return common.Bytes4{0xd1, byte(epoch >> 16), byte(epoch >> 8), byte(epoch)}
}

// newEnvelopeContentsFixture returns a fixture whose block is at slot, or at the current slot when
// slot is 0.
func newEnvelopeContentsFixture(t *testing.T, slot uint64, blobCount int) *envelopeContentsFixture {
	t.Helper()
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg)
	}
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	cfg.GloasForkEpoch = 0
	if slot == 0 {
		slot = handler.ethClock.GetCurrentSlot()
	}
	f := &envelopeContentsFixture{t: t, handler: handler, forkchoice: fcu, slot: slot, currentSlot: slot, consensusCheckBlobData: true}

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 { return f.currentSlot }).AnyTimes()
	clock.EXPECT().CurrentForkDigest().Return(wallClockForkDigest, nil).AnyTimes()
	clock.EXPECT().ComputeForkDigest(gomock.Any()).DoAndReturn(func(epoch uint64) (common.Bytes4, error) {
		f.digestEpochs = append(f.digestEpochs, epoch)
		return forkDigestOfEpoch(epoch), nil
	}).AnyTimes()
	handler.ethClock = clock
	handler.sentinel = &nonNilSentinelClient{}
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, data []byte) error {
			return f.recordPublish(ctx, wallClockForkDigest, topic, data)
		},
	).AnyTimes()
	gossipManager.EXPECT().PublishToForkDigest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest common.Bytes4, topic string, data []byte) error {
			return f.recordPublish(ctx, digest, topic, data)
		},
	).AnyTimes()
	handler.gossipManager = gossipManager

	columnEmitter := beaconevents.NewEventEmitter()
	f.columnEvents = make(chan *beaconevents.EventStream, 2*int(cfg.NumberOfColumns))
	subscription := columnEmitter.Operation().Subscribe(f.columnEvents)
	t.Cleanup(subscription.Unsubscribe)
	store := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), cfg, columnEmitter)
	handler.columnStorage = &loggingColumnStorage{DataColumnStorage: store, f: f}
	handler.peerDas = (&publishedColumnsFixture{handler: handler, columns: &hookedColumnStorage{DataColumnStorage: store}}).newPeerDas(t, clparams.CaplinConfig{})
	custody, err := handler.peerDas.StateReader().GetMyCustodyColumns()
	require.NoError(t, err)
	require.Len(t, custody, 8, "the custody must be smaller than the column set, or storing only custody could not be told from storing all")

	for i := range blobCount {
		f.blobs = append(f.blobs, newTestBlob(t, byte(i+1), clparams.FuluVersion))
	}

	executionRequests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	executionRequestsRoot, err := executionRequests.HashSSZ()
	require.NoError(t, err)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.BlockHash = common.HexToHash("0x1234")
	payload.SlotNumber = slot
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)

	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BuilderIndex = 3
	bid.Slot = slot
	bid.BlockHash = payload.BlockHash
	bid.ExecutionRequestsRoot = common.Hash(executionRequestsRoot)
	bundles := make([]BlobBundle, len(f.blobs))
	for i, b := range f.blobs {
		commitment := b.commitment
		bid.BlobKzgCommitments.Append(&commitment)
		bundles[i] = bundleOf(b)
	}
	_, ok := handler.pendingBuilderPayloads.Add(slot, bid, &selfBuildPayload{Payload: payload, ExecutionRequests: executionRequests, BlobBundles: bundles})
	require.True(t, ok)
	f.root, err = block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Blocks[f.root] = block
	f.signedEnvelope = &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload: payload, ExecutionRequests: executionRequests, BuilderIndex: bid.BuilderIndex,
		BeaconBlockRoot: f.root, ParentBeaconBlockRoot: block.Block.ParentRoot,
	}}

	fcu.OnExecutionPayloadFn = func(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
		f.record(contentsEvent{kind: eventImport})
		if !f.dataAvailable() {
			return forkchoice.ErrEIP7594ColumnDataNotAvailable
		}
		if fcu.Envelopes[f.root] != nil {
			return forkchoice.ErrIgnore
		}
		fcu.Envelopes[f.root] = envelope
		return nil
	}
	handler.forkchoiceStore = &dataAvailabilityForkchoice{ForkChoiceStorageMock: fcu, f: f}
	return f
}

func (f *envelopeContentsFixture) dataAvailable() bool {
	if len(f.blobs) == 0 {
		return true
	}
	available, err := f.handler.peerDas.IsDataAvailable(f.slot, f.root)
	require.NoError(f.t, err)
	return available
}

func (f *envelopeContentsFixture) record(event contentsEvent) {
	f.events = append(f.events, event)
}

func (f *envelopeContentsFixture) recordPublish(ctx context.Context, forkDigest common.Bytes4, topic string, data []byte) error {
	event := contentsEvent{kind: eventEnvelopePublish, forkDigest: forkDigest, err: f.failEnvelope}
	if topic != gossip.TopicNameExecutionPayload {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		require.NoError(f.t, sidecar.DecodeSSZ(data, int(clparams.GloasVersion)))
		event = contentsEvent{kind: eventColumnPublish, index: sidecar.Index, forkDigest: forkDigest, data: data}
		if f.failPublish != nil {
			event.err = f.failPublish(sidecar.Index)
		}
	}
	if event.err == nil {
		event.err = ctx.Err()
	}
	event.deadline, _ = ctx.Deadline()
	f.record(event)
	return event.err
}

func (f *envelopeContentsFixture) eventsOf(kind contentsEventKind) []contentsEvent {
	return slices.DeleteFunc(slices.Clone(f.events), func(e contentsEvent) bool { return e.kind != kind })
}

func (f *envelopeContentsFixture) positions(kind contentsEventKind) []int {
	var positions []int
	for i, e := range f.events {
		if e.kind == kind {
			positions = append(positions, i)
		}
	}
	return positions
}

// succeededIndices returns the sorted column indices of the events of kind that did not fail.
func (f *envelopeContentsFixture) succeededIndices(kind contentsEventKind) []uint64 {
	var indices []uint64
	for _, e := range f.eventsOf(kind) {
		if e.err == nil {
			indices = append(indices, e.index)
		}
	}
	slices.Sort(indices)
	return indices
}

func (f *envelopeContentsFixture) newColumnEvents() int {
	count := 0
	for len(f.columnEvents) > 0 {
		if (<-f.columnEvents).Event == beaconevents.OpDataColumnSidecar {
			count++
		}
	}
	return count
}

func (f *envelopeContentsFixture) requireAllColumns(what string, got []uint64) {
	f.t.Helper()
	columns := f.handler.beaconChainCfg.NumberOfColumns
	want := make([]uint64, columns)
	for i := range want {
		want[i] = uint64(i)
	}
	require.Truef(f.t, slices.Equal(want, got), "%s %d of %d distinct columns", what, len(got), columns)
}

func (f *envelopeContentsFixture) requireNoEvents() {
	f.t.Helper()
	require.Empty(f.t, f.events, "nothing may be published or written")
}

// contents returns the request contents for blobs with the proofs they were built with.
func (f *envelopeContentsFixture) contents(blobs ...testBlob) *cltypes.SignedExecutionPayloadEnvelopeContents {
	contents := cltypes.NewSignedExecutionPayloadEnvelopeContents(f.handler.beaconChainCfg, f.slot)
	contents.SignedExecutionPayloadEnvelope = f.signedEnvelope
	for _, b := range blobs {
		contents.Blobs.Append(b.blob)
		for i := range b.proofs {
			contents.KZGProofs.Append(&b.proofs[i])
		}
	}
	return contents
}

func (f *envelopeContentsFixture) postContents(ctx context.Context, contents *cltypes.SignedExecutionPayloadEnvelopeContents, query string) *httptest.ResponseRecorder {
	f.t.Helper()
	body, err := contents.EncodeSSZ(nil)
	require.NoError(f.t, err)
	return f.post(ctx, body, "application/octet-stream", "true", query)
}

// postAllBlobs posts the envelope with all fixture blobs. With pendingLocal it posts the bare envelope,
// so the handler attaches the contents it holds for the bid.
func (f *envelopeContentsFixture) postAllBlobs(pendingLocal bool, query string) *httptest.ResponseRecorder {
	f.t.Helper()
	return f.postAllBlobsWithContext(f.t.Context(), pendingLocal, query)
}

func (f *envelopeContentsFixture) postAllBlobsWithContext(ctx context.Context, pendingLocal bool, query string) *httptest.ResponseRecorder {
	f.t.Helper()
	if !pendingLocal {
		return f.postContents(ctx, f.contents(f.blobs...), query)
	}
	body, err := json.Marshal(f.signedEnvelope)
	require.NoError(f.t, err)
	return f.post(ctx, body, "application/json", "false", query)
}

func (f *envelopeContentsFixture) post(ctx context.Context, body []byte, contentType, blobDataIncluded, query string) *httptest.ResponseRecorder {
	return f.postReader(ctx, bytes.NewReader(body), contentType, blobDataIncluded, query)
}

func (f *envelopeContentsFixture) postReader(ctx context.Context, body io.Reader, contentType, blobDataIncluded, query string) *httptest.ResponseRecorder {
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes"+query, body)
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Eth-Blob-Data-Included", blobDataIncluded)
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()
	f.handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
	return recorder
}

// expectedColumns returns the encoded columns the network must receive for blobs.
func (f *envelopeContentsFixture) expectedColumns(blobs ...testBlob) [][]byte {
	f.t.Helper()
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(blobs))
	for i, b := range blobs {
		cells, err := peerdasutils.ComputeCells(b.blob)
		require.NoError(f.t, err)
		cellsAndProofs[i] = peerdasutils.CellsAndKZGProofs{Blobs: cells, Proofs: b.proofs}
	}
	columns, err := peerdasutils.GetDataColumnSidecarsGloas(f.handler.beaconChainCfg, f.slot, f.root, cellsAndProofs)
	require.NoError(f.t, err)
	encoded := make([][]byte, len(columns))
	for i, column := range columns {
		encoded[i], err = column.EncodeSSZ(nil)
		require.NoError(f.t, err)
	}
	return encoded
}

func (f *envelopeContentsFixture) requirePublishedColumnsEqual(want [][]byte) {
	f.t.Helper()
	published := 0
	for _, e := range f.eventsOf(eventColumnPublish) {
		if e.err != nil {
			continue
		}
		published++
		require.Equal(f.t, want[e.index], e.data, "column %d", e.index)
	}
	require.Equal(f.t, len(want), published)
}

func TestPostEnvelopeContentsPublishesColumnsBeforeFirstWrite(t *testing.T) {
	for _, mode := range []string{"stateless", "pendingLocal"} {
		t.Run(mode, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			cfg := f.handler.beaconChainCfg
			recorder := f.postAllBlobs(mode == "pendingLocal", "")

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			publishes, writes := f.positions(eventColumnPublish), f.positions(eventColumnWrite)
			require.Len(t, publishes, int(cfg.NumberOfColumns))
			require.NotEmpty(t, writes)
			require.Less(t, publishes[len(publishes)-1], writes[0], "every column must be published before the first write")
			f.requireAllColumns("written", f.succeededIndices(eventColumnWrite))
			require.Equal(t, int(cfg.NumberOfColumns), f.newColumnEvents())
		})
	}
}

func TestPostEnvelopeContentsWriteFailureStillPublishesColumns(t *testing.T) {
	for _, tc := range []struct {
		name          string
		pendingLocal  bool
		query         string
		legacy        bool
		status        int
		envelopes     int
		envelopeFirst bool
	}{
		{name: "gossip with blob data", status: http.StatusAccepted, envelopes: 1, envelopeFirst: true},
		{name: "pendingLocal", pendingLocal: true, status: http.StatusAccepted, envelopes: 1, envelopeFirst: true},
		{name: "consensus", query: "?broadcast_validation=consensus", status: http.StatusAccepted, envelopes: 1},
		{name: "consensus_and_equivocation", query: "?broadcast_validation=consensus_and_equivocation", status: http.StatusAccepted, envelopes: 1},
		{name: "consensus with pendingLocal", pendingLocal: true, query: "?broadcast_validation=consensus", status: http.StatusAccepted, envelopes: 1},
		{name: "legacy route", legacy: true, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failWrite = func(uint64) error { return errors.New("injected write failure") }
			var recorder *httptest.ResponseRecorder
			if tc.legacy {
				recorder = f.postLegacy(t.Context())
			} else {
				recorder = f.postAllBlobs(tc.pendingLocal, tc.query)
			}

			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
			require.Len(t, f.eventsOf(eventEnvelopePublish), tc.envelopes)
			if tc.envelopeFirst {
				require.Equal(t, eventEnvelopePublish, f.events[0].kind)
			} else if tc.envelopes == 1 {
				require.Equal(t, eventEnvelopePublish, f.events[len(f.events)-1].kind)
			}
			requireNotCached(t, f.handler, f.blobs...)
		})
	}
}

func TestPostEnvelopeContentsPublishesColumnsAfterClientCancel(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 2)
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	f.failPublish = func(uint64) error { cancelRequest(); return nil }
	slotDuration := time.Duration(f.handler.beaconChainCfg.SecondsPerSlot) * time.Second

	start := time.Now()
	f.postContents(requestCtx, f.contents(f.blobs...), "")
	end := time.Now()

	f.requireAllColumns("published with a live context", f.succeededIndices(eventColumnPublish))
	publishes := f.eventsOf(eventColumnPublish)
	for _, e := range publishes {
		require.False(t, e.deadline.IsZero(), "column %d was published without a deadline", e.index)
		require.Equal(t, publishes[0].deadline, e.deadline, "all columns share one deadline")
	}
	require.False(t, publishes[0].deadline.Before(start.Add(slotDuration)))
	require.False(t, publishes[0].deadline.After(end.Add(slotDuration)))
}

func TestPostEnvelopeContentsPublishesColumnsUnderTheBlockEpochDigest(t *testing.T) {
	const epoch = 3
	slotsPerEpoch := clparams.MainnetBeaconConfig.SlotsPerEpoch
	for _, tc := range []struct {
		name string
		slot uint64
	}{
		{"last slot before the boundary", epoch*slotsPerEpoch - 1},
		{"first slot of the epoch", epoch * slotsPerEpoch},
		{"slot after the boundary", epoch*slotsPerEpoch + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, tc.slot, 2)
			f.currentSlot = tc.slot + slotsPerEpoch
			blockEpoch := tc.slot / slotsPerEpoch

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), "")

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			publishes := f.eventsOf(eventColumnPublish)
			require.Len(t, publishes, int(f.handler.beaconChainCfg.NumberOfColumns))
			wantDigest := forkDigestOfEpoch(blockEpoch)
			for _, e := range publishes {
				require.Equal(t, wantDigest, e.forkDigest, "column %d", e.index)
			}
			require.Equal(t, []uint64{blockEpoch}, f.digestEpochs, "the digest is computed once, for the block's epoch")
		})
	}
}

func TestPostEnvelopeContentsColumnPublishFailureAllowsRepairingRePost(t *testing.T) {
	const failedColumn = 5
	for _, tc := range []struct {
		name, query   string
		envelopeFirst bool
	}{
		{name: "gossip", envelopeFirst: true},
		{name: "consensus", query: "?broadcast_validation=consensus"},
		{name: "consensus_and_equivocation", query: "?broadcast_validation=consensus_and_equivocation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			cfg := f.handler.beaconChainCfg
			f.failPublish = func(index uint64) error {
				if index == failedColumn {
					return errors.New("injected publish failure")
				}
				return nil
			}
			body := f.contents(f.blobs...)

			first := f.postContents(t.Context(), body, tc.query)

			require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
			require.Len(t, f.succeededIndices(eventColumnPublish), int(cfg.NumberOfColumns)-1)
			require.NotContains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn))
			envelopes := f.positions(eventEnvelopePublish)
			writes := f.positions(eventColumnWrite)
			require.Len(t, envelopes, 1, "a failed column must not withhold the envelope")
			require.Len(t, writes, int(cfg.NumberOfColumns))
			if tc.envelopeFirst {
				require.Less(t, envelopes[0], writes[0], "the envelope goes out before the columns are stored")
			} else {
				require.Less(t, writes[len(writes)-1], envelopes[0], "all columns are stored before the envelope goes out")
			}
			for _, b := range f.blobs {
				_, ok := f.handler.blobBundles.Get(common.Bytes48(b.commitment))
				require.True(t, ok, "the bundles are cached before the response")
			}

			if tc.envelopeFirst {
				require.True(t, f.forkchoice.HasEnvelope(f.root), "the retry must find the envelope persisted")
			}

			f.failPublish = nil
			published := len(f.eventsOf(eventColumnPublish))
			second := f.postContents(t.Context(), body, tc.query)

			require.Equal(t, http.StatusOK, second.Code, second.Body.String())
			f.requireAllColumns("published after the re-POST", slices.Compact(f.succeededIndices(eventColumnPublish)))
			if tc.envelopeFirst {
				require.Len(t, f.eventsOf(eventColumnPublish), published+int(cfg.NumberOfColumns))
			}
			require.Len(t, f.eventsOf(eventEnvelopePublish), 1, "the re-POST must not publish the envelope again")

			events := len(f.events)
			third := f.postContents(t.Context(), body, tc.query)

			if tc.envelopeFirst {
				require.Equal(t, http.StatusOK, third.Code, third.Body.String())
			} else {
				require.Equal(t, http.StatusBadRequest, third.Code, "a repaired re-POST must clear the retry record")
			}
			require.Len(t, f.events, events, "a repaired re-POST must clear the retry record, so nothing is published again")
		})
	}
}

func TestPostEnvelopeContentsColumnPublishFailureWinsOverWriteFailure(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failWrite = func(uint64) error { return errors.New("injected write failure") }
	f.failPublish = func(index uint64) error {
		if index == 5 {
			return errors.New("injected publish failure")
		}
		return nil
	}

	recorder := f.postContents(t.Context(), f.contents(f.blobs...), "")

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
}

func TestPostEnvelopeContentsLogsBlockOfFailedColumnPublish(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 1)
	var logged bytes.Buffer
	f.handler.logger = log.New()
	f.handler.logger.SetHandler(log.SyncHandler(log.FilterHandler(
		func(r *log.Record) bool { return r.Msg == "failed to publish execution payload data columns" },
		log.StreamHandler(&logged, log.LogfmtFormat()),
	)))
	f.failPublish = func(index uint64) error {
		if index == 5 {
			return errors.New("injected publish failure")
		}
		return nil
	}

	f.postContents(t.Context(), f.contents(f.blobs...), "")

	require.Contains(t, logged.String(), fmt.Sprintf("slot=%d", f.slot))
	require.Contains(t, logged.String(), "blockRoot="+f.root.String())
}

func TestPostEnvelopeContentsUsesCachedLocalBundle(t *testing.T) {
	for _, query := range []string{"", "?broadcast_validation=consensus", "?broadcast_validation=consensus_and_equivocation"} {
		t.Run(query, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 1)
			f.handler.blobBundles.Add(common.Bytes48(f.blobs[0].commitment), bundleOf(f.blobs[0]))
			other := newTestBlob(t, 99, clparams.FuluVersion)
			request := testBlob{blob: other.blob, commitment: f.blobs[0].commitment, proofs: make([]cltypes.KZGProof, len(other.proofs))}

			recorder := f.postContents(t.Context(), f.contents(request), query)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, query == "", f.consensusCheckBlobData)
			f.requirePublishedColumnsEqual(f.expectedColumns(f.blobs...))
		})
	}
}

func TestPostEnvelopeContentsStoredColumnsPassDataAvailability(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pendingLocal bool
		query        string
	}{
		{name: "gossip"},
		{name: "consensus", query: "?broadcast_validation=consensus"},
		{name: "pendingLocal", pendingLocal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			recorder := f.postAllBlobs(tc.pendingLocal, tc.query)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.True(t, f.forkchoice.OnExecutionPayloadCalled)
		})
	}
}

func TestPostEnvelopeContentsRejectsInvalidProofBeforePublishing(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 2)
	contents := f.contents(f.blobs...)
	contents.KZGProofs.Set(7, &f.blobs[1].proofs[7])

	recorder := f.postContents(t.Context(), contents, "")

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	f.requireNoEvents()
	requireNotCached(t, f.handler, f.blobs...)
}

func TestPostEnvelopeContentsRejectsBlobOfAnotherCommitment(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 1)
	other := newTestBlob(t, 99, clparams.FuluVersion)

	recorder := f.postContents(t.Context(), f.contents(other), "")

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	f.requireNoEvents()
	requireNotCached(t, f.handler, f.blobs...)
	requireNotCached(t, f.handler, other)
}

func TestPostEnvelopeContentsRejectsNilElements(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cached bool
		nilOut func(*envelopeContentsFixture, *cltypes.SignedExecutionPayloadEnvelopeContents)
	}{
		{name: "commitment", nilOut: func(f *envelopeContentsFixture, _ *cltypes.SignedExecutionPayloadEnvelopeContents) {
			bid := f.forkchoice.Blocks[f.root].Block.Body.GetSignedExecutionPayloadBid().Message
			bid.BlobKzgCommitments.Set(1, nil)
		}},
		{name: "blob", nilOut: func(_ *envelopeContentsFixture, c *cltypes.SignedExecutionPayloadEnvelopeContents) {
			c.Blobs.Set(1, nil)
		}},
		{name: "proof", nilOut: func(_ *envelopeContentsFixture, c *cltypes.SignedExecutionPayloadEnvelopeContents) {
			c.KZGProofs.Set(130, nil)
		}},
		{name: "blob of a cached bundle", cached: true, nilOut: func(_ *envelopeContentsFixture, c *cltypes.SignedExecutionPayloadEnvelopeContents) {
			c.Blobs.Set(0, nil)
		}},
		{name: "proof of a cached bundle", cached: true, nilOut: func(_ *envelopeContentsFixture, c *cltypes.SignedExecutionPayloadEnvelopeContents) {
			c.KZGProofs.Set(5, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			if tc.cached {
				f.handler.blobBundles.Add(common.Bytes48(f.blobs[0].commitment), bundleOf(f.blobs[0]))
			}
			contents := f.contents(f.blobs...)
			tc.nilOut(f, contents)

			var err error
			require.NotPanics(t, func() { _, err = f.handler.validateExecutionPayloadEnvelopeContents(t.Context(), contents) })

			require.Error(t, err)
			require.NotErrorIs(t, err, errExecutionPayloadEnvelopeIntegration, "invalid input must be answered with 400, not 202")
			f.requireNoEvents()
			requireNotCached(t, f.handler, f.blobs[1])
		})
	}
}

func TestPostEnvelopeContentsTreatsCachedNilBlobAsMiss(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 1)
	cached := bundleOf(f.blobs[0])
	cached.Blob = nil
	f.handler.blobBundles.Add(cached.Commitment, cached)

	recorder := f.postContents(t.Context(), f.contents(f.blobs...), "")

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	f.requirePublishedColumnsEqual(f.expectedColumns(f.blobs...))
}

func TestPostEnvelopeContentsCachedBundleWithWrongProofCountIsVerified(t *testing.T) {
	cachedOneProof := func(f *envelopeContentsFixture) {
		cached := bundleOf(f.blobs[0])
		cached.KzgProofs = cached.KzgProofs[:1]
		f.handler.blobBundles.Add(cached.Commitment, cached)
	}

	t.Run("valid request", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 1)
		cachedOneProof(f)

		recorder := f.postContents(t.Context(), f.contents(f.blobs...), "")

		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		f.requirePublishedColumnsEqual(f.expectedColumns(f.blobs...))
	})
	t.Run("corrupted request", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 1)
		cachedOneProof(f)
		contents := f.contents(f.blobs...)
		contents.KZGProofs.Set(3, &f.blobs[0].proofs[4])

		recorder := f.postContents(t.Context(), contents, "")

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		f.requireNoEvents()
	})
}

func TestPostEnvelopeContentsMixesCachedAndRequestBundles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cached int
	}{
		{"cached then request", 0},
		{"request then cached", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.handler.blobBundles.Add(common.Bytes48(f.blobs[tc.cached].commitment), bundleOf(f.blobs[tc.cached]))

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), "")

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			f.requirePublishedColumnsEqual(f.expectedColumns(f.blobs...))
		})
	}
}

func TestPostEnvelopeContentsConsensusModesRetryOfIgnoredEnvelope(t *testing.T) {
	const failedColumn = 5
	importIgnoringAfterPersisting := func(f *envelopeContentsFixture, persisted *cltypes.SignedExecutionPayloadEnvelope) {
		f.forkchoice.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
			f.forkchoice.SetEnvelope(f.root, persisted)
			return forkchoice.ErrIgnore
		}
	}
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		query := "?broadcast_validation=" + mode
		t.Run(mode+"/a fresh claim keeps 400", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			importIgnoringAfterPersisting(f, f.signedEnvelope)

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), query)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.True(t, f.forkchoice.OnExecutionPayloadCalled)
			require.Empty(t, f.eventsOf(eventEnvelopePublish))
		})
		t.Run(mode+"/a fresh claim with a different persisted envelope keeps 400", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			importIgnoringAfterPersisting(f, f.differentEnvelope())

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), query)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.True(t, f.forkchoice.OnExecutionPayloadCalled)
			require.Empty(t, f.eventsOf(eventEnvelopePublish))
		})
		t.Run(mode+"/a different persisted envelope answers 503", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failPublish = func(index uint64) error {
				if index == failedColumn {
					return errors.New("injected publish failure")
				}
				return nil
			}
			body := f.contents(f.blobs...)
			first := f.postContents(t.Context(), body, query)
			require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
			_, retryRecorded := f.handler.executionPayloadEnvelopeRetries.Peek(executionPayloadEnvelopeGossipKey{
				BeaconBlockRoot: f.root, BuilderIndex: f.signedEnvelope.Message.BuilderIndex,
			})
			require.True(t, retryRecorded)

			f.failPublish = nil
			importIgnoringAfterPersisting(f, f.differentEnvelope())
			second := f.postContents(t.Context(), body, query)

			require.Equal(t, http.StatusServiceUnavailable, second.Code, second.Body.String())
			require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn))
			require.Len(t, f.eventsOf(eventEnvelopePublish), 1, "only the first POST publishes the envelope")
		})
	}
}
