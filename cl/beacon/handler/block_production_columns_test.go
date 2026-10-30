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
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	clservices "github.com/erigontech/erigon/cl/phase1/network/services"
	network_services_mock "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
)

// publishedColumnsFixture publishes a block with one blob through a handler that imports it into fork
// choice, stores its columns in a real column store and runs the store job on its own goroutine, so
// -race sees the store job and column gossip run concurrently.
type publishedColumnsFixture struct {
	handler    *ApiHandler
	forkchoice *installingForkchoice
	columns    *hookedColumnStorage
	published  *publishedGossip
	// Closed by the gossip goroutine. Polling the recorded topics instead would share their mutex with
	// that goroutine and, through the test goroutine, order the store's writes before later gossip,
	// which hides a store/gossip race from -race.
	columnsGossiped chan struct{}
	block           *cltypes.SignedBeaconBlock
	root            common.Hash
	store           func(context.Context) error
}

type hookedColumnStorage struct {
	blob_storage.DataColumnStorage
	hook func(write func() error) error
}

func (s *hookedColumnStorage) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, sidecar *cltypes.DataColumnSidecar) error {
	write := func() error {
		return s.DataColumnStorage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	}
	if s.hook == nil {
		return write()
	}
	return s.hook(write)
}

type runningPublishedBlockJob struct {
	done chan struct{}
	err  error
}

func (j *runningPublishedBlockJob) Wait(ctx context.Context) error {
	select {
	case <-j.done:
		return j.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newPublishedColumnsFixture(t *testing.T, version clparams.StateVersion) *publishedColumnsFixture {
	t.Helper()
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg)
	}
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	f := &publishedColumnsFixture{
		handler:         handler,
		published:       &publishedGossip{topics: map[string][]byte{}},
		columnsGossiped: make(chan struct{}),
	}
	ctrl := gomock.NewController(t)

	b := newTestBlob(t, 1, clparams.FuluVersion)
	proofs := make([]common.Bytes48, len(b.proofs))
	for i := range b.proofs {
		proofs[i] = common.Bytes48(b.proofs[i])
	}
	handler.blobBundles.Add(common.Bytes48(b.commitment), BlobBundle{Commitment: common.Bytes48(b.commitment), Blob: b.blob, KzgProofs: proofs})
	f.block = cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, version)
	f.block.Block.Slot = blocks[1].Block.Slot
	commitment := b.commitment
	if version >= clparams.GloasVersion {
		bid := f.block.Block.Body.SignedExecutionPayloadBid.Message
		bid.BuilderIndex = clparams.BuilderIndexSelfBuild
		bid.BlobKzgCommitments.Append(&commitment)
	} else {
		f.block.Block.Body.BlobKzgCommitments.Append(&commitment)
	}
	root, err := f.block.Block.HashSSZ()
	require.NoError(t, err)
	f.root = root

	fcu.StateAtBlockRootVal[f.root] = postState
	fcu.HeadVal = f.root
	fcu.HeadSlotVal = f.block.Block.Slot
	fcu.OnTickFn = func(uint64) {}
	f.forkchoice = &installingForkchoice{
		ForkChoiceStorage: fcu,
		headers:           make(map[common.Hash]*cltypes.BeaconBlockHeader),
		blocks:            make(map[common.Hash]*cltypes.SignedBeaconBlock),
	}
	handler.forkchoiceStore = f.forkchoice
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	handler.engine = engine
	handler.attestationProducer = cachedAttestationDataProducer{}

	gossipManager := gossip_mock.NewMockGossip(ctrl)
	var allColumnsGossiped sync.Once
	gossipManager.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topic string, data []byte) error {
		if err := f.published.record(ctx, topic, data); err != nil {
			return err
		}
		if f.published.columnCount() == int(handler.beaconChainCfg.NumberOfColumns) {
			allColumnsGossiped.Do(func() { close(f.columnsGossiped) })
		}
		return nil
	}).AnyTimes()
	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.gossipManager = gossipManager
	blockService := network_services_mock.NewMockBlockService(ctrl)
	blockService.EXPECT().ValidateGossip(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	blockService.EXPECT().CommitGossipReservation(gomock.Any()).AnyTimes()
	blockService.EXPECT().ReleaseGossipReservation(gomock.Any()).AnyTimes()
	blockService.EXPECT().SchedulePublishedBlockForLaterProcessing(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ *cltypes.SignedBeaconBlock, store func(context.Context) error) clservices.PublishedBlockJob {
			f.store = store
			job := &runningPublishedBlockJob{done: make(chan struct{})}
			go func() {
				defer close(job.done)
				job.err = store(context.Background())
			}()
			return job
		},
	).AnyTimes()
	handler.blockService = blockService

	f.columns = &hookedColumnStorage{DataColumnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), handler.beaconChainCfg, beaconevents.NewEventEmitter())}
	handler.columnStorage = f.columns
	handler.peerDas = f.newPeerDas(t, clparams.CaplinConfig{})
	return f
}

func (f *publishedColumnsFixture) newPeerDas(t *testing.T, caplinConfig clparams.CaplinConfig) das.PeerDas {
	t.Helper()
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	state := peerdasstate.NewPeerDasState(f.handler.beaconChainCfg, &networkConfig)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(nodeDB.Close)
	localNode := enode.NewLocalNode(nodeDB, key)
	state.SetLocalNodeID(localNode)
	return das.NewPeerDas(nil, f.handler.beaconChainCfg, &caplinConfig, f.columns, nil, nil, localNode.ID(), f.handler.ethClock, state, f.handler.gossipManager, nil, f.handler.indiciesDB)
}

func (f *publishedColumnsFixture) blockIndexed() (bool, error) {
	var slot *uint64
	err := f.handler.indiciesDB.View(context.Background(), func(tx kv.Tx) error {
		var err error
		slot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, f.root)
		return err
	})
	return slot != nil, err
}

func (f *publishedColumnsFixture) storedColumns(t *testing.T) []uint64 {
	t.Helper()
	saved, err := f.columns.GetSavedColumnIndex(t.Context(), f.block.Block.Slot, f.root)
	require.NoError(t, err)
	slices.Sort(saved)
	return saved
}

func (f *publishedColumnsFixture) custodyColumns(t *testing.T) []uint64 {
	t.Helper()
	custody, err := f.handler.peerDas.StateReader().GetMyCustodyColumns()
	require.NoError(t, err)
	return slices.Sorted(maps.Keys(custody))
}

func requireClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

func (p *publishedGossip) columnCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	columns := 0
	for topic := range p.topics {
		if strings.HasPrefix(topic, "data_column_sidecar_") {
			columns++
		}
	}
	return columns
}

func TestBroadcastBlockStoresAllColumnsInArchiveMode(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			f := newPublishedColumnsFixture(t, version)
			f.handler.peerDas = f.newPeerDas(t, clparams.CaplinConfig{ArchiveBlobs: true})

			require.NoError(t, f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip))

			require.Len(t, f.storedColumns(t), int(f.handler.beaconChainCfg.NumberOfColumns))
		})
	}
}

func TestBroadcastBlockWritesColumnsAfterOnBlockAndIndexWrite(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			f := newPublishedColumnsFixture(t, version)
			var once sync.Once
			var onBlockCalls int
			var indexed bool
			var indexErr error
			f.columns.hook = func(write func() error) error {
				once.Do(func() {
					onBlockCalls = f.forkchoice.onBlockCalls
					indexed, indexErr = f.blockIndexed()
				})
				return write()
			}

			require.NoError(t, f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip))

			require.NoError(t, indexErr)
			require.Equal(t, 1, onBlockCalls, "OnBlock must run before the first column write")
			require.True(t, indexed, "the block index must be written before the first column write")
		})
	}
}

// TestBroadcastGloasBlockHoldsColumnsUntilBlockIsServable proves Gloas column gossip waits for the
// block to be imported, so a peer that asks for the block of a column can get it, and that a stalled
// import holds the columns back for at most publishedBlockStoreWaitBeforeColumns.
func TestBroadcastGloasBlockHoldsColumnsUntilBlockIsServable(t *testing.T) {
	f := newPublishedColumnsFixture(t, clparams.GloasVersion)
	importing, release := make(chan struct{}), make(chan struct{})
	f.forkchoice.afterOnBlock = func() {
		close(importing)
		<-release
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip) }()
	defer func() {
		close(release)
		require.NoError(t, <-done)
	}()

	requireClosed(t, importing, "the block was not imported")
	requireClosed(t, f.columnsGossiped, "the columns were not gossiped")
	require.GreaterOrEqual(t, time.Since(start), publishedBlockStoreWaitBeforeColumns, "columns must wait for the block import")
}

func TestBroadcastGloasBlockGossipsColumnsOnceTheStoreJobFails(t *testing.T) {
	f := newPublishedColumnsFixture(t, clparams.GloasVersion)
	f.handler.forkchoiceStore = &rejectingOnBlockForkchoice{ForkChoiceStorage: f.forkchoice, err: forkchoice.ErrBlockInvalid}
	start := time.Now()

	require.ErrorIs(t, f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip), forkchoice.ErrBlockInvalid)

	requireClosed(t, f.columnsGossiped, "the columns were not gossiped")
	require.Less(t, time.Since(start), publishedBlockStoreWaitBeforeColumns, "a failed store job must release the columns")
}

func TestBroadcastGloasBlockGossipsColumnsWhileColumnWriteBlocks(t *testing.T) {
	f := newPublishedColumnsFixture(t, clparams.GloasVersion)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.columns.hook = func(write func() error) error {
		err := write()
		once.Do(func() { close(blocked) })
		<-release
		return err
	}
	done := make(chan error, 1)
	go func() { done <- f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip) }()
	defer func() {
		close(release)
		require.NoError(t, <-done)
	}()

	requireClosed(t, blocked, "no column was written")
	blockedAt := time.Now()
	requireClosed(t, f.columnsGossiped, "the columns were not gossiped")
	require.Less(t, time.Since(blockedAt), publishedBlockStoreWaitBeforeColumns/2, "column gossip must not wait for the column writes")
}

func TestBroadcastBlockColumnWriteFailureKeepsImportedBlockAndRetries(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			f := newPublishedColumnsFixture(t, version)
			var failing atomic.Bool
			failing.Store(true)
			f.columns.hook = func(write func() error) error {
				if failing.Load() {
					return errors.New("disk full")
				}
				return write()
			}

			var writeErr *clservices.AcceptedBlockWriteError
			require.ErrorAs(t, f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip), &writeErr,
				"the columns of an accepted block must be retried past the job expiry")

			indexed, err := f.blockIndexed()
			require.NoError(t, err)
			require.True(t, indexed, "a block accepted by fork choice must be persisted although its columns failed")

			failing.Store(false)
			require.NoError(t, f.store(t.Context()), "the block service retries the same store job")
			require.Equal(t, 1, f.forkchoice.onBlockCalls, "the retry must not import the block again")
			require.Equal(t, f.custodyColumns(t), f.storedColumns(t))
		})
	}
}

// TestBroadcastBlockStoresCustodyRaisedByItsOwnImport proves the columns are chosen after OnBlock,
// which may raise the custody group count.
func TestBroadcastBlockStoresCustodyRaisedByItsOwnImport(t *testing.T) {
	const raisedCgc = 16
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			f := newPublishedColumnsFixture(t, version)
			before := f.custodyColumns(t)
			f.forkchoice.afterOnBlock = func() { f.handler.peerDas.UpdateValidatorsCustody(raisedCgc) }

			require.NoError(t, f.handler.broadcastBlock(t.Context(), f.block, BlockPublishingValidationGossip))

			raised := f.custodyColumns(t)
			require.Len(t, raised, raisedCgc)
			require.Less(t, len(before), len(raised))
			require.Equal(t, raised, f.storedColumns(t))
			require.Equal(t, int(f.handler.beaconChainCfg.NumberOfColumns), f.published.columnCount(), "every column is gossiped")
		})
	}
}

// TestPostGloasBlockWithoutIntegrationWaitLetsStatefulEnvelopePassDA covers the v1 block POST, which
// does not wait for the store job: a stateful envelope POST that follows must find the custody columns.
func TestPostGloasBlockWithoutIntegrationWaitLetsStatefulEnvelopePassDA(t *testing.T) {
	f := newPublishedColumnsFixture(t, clparams.GloasVersion)
	imported := make(chan struct{})
	f.forkchoice.afterOnBlock = func() { close(imported) }
	f.columns.hook = func(write func() error) error {
		time.Sleep(50 * time.Millisecond)
		return write()
	}
	f.forkchoice.ForkChoiceStorage.(*forkchoice_mock.ForkChoiceStorageMock).OnExecutionPayloadFn = func(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
		available, err := f.handler.peerDas.IsDataAvailable(f.block.Block.Slot, envelope.Message.BeaconBlockRoot)
		if err != nil {
			return err
		}
		if !available {
			return forkchoice.ErrEIP7594ColumnDataNotAvailable
		}
		return nil
	}

	require.NoError(t, f.handler.broadcastBlockWithIntegrationWait(t.Context(), f.block, BlockPublishingValidationGossip, false))
	requireClosed(t, imported, "the block was not imported")

	body := fmt.Sprintf(`{"message":{"beacon_block_root":"%s","builder_index":"%d"}}`, f.root.Hex(), uint64(clparams.BuilderIndexSelfBuild))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()
	f.handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}
