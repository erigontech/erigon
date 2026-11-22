package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	st "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	gossipmgr "github.com/erigontech/erigon/cl/phase1/network/gossip"
	serviceinterface "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// pendingGloasSidecarExpiry is how long to keep pending sidecars before expiring (2 slots)
	pendingGloasSidecarExpiry = 24 * time.Second // ~2 slots at 12s per slot
	// pendingGloasSidecarTick is how often to check pending sidecars
	pendingGloasSidecarTick = 500 * time.Millisecond
	// maxPendingGloasSidecars caps the number of sidecars queued for later processing
	maxPendingGloasSidecars = 4096
	// maxPendingGloasVerificationsPerPass bounds the KZG work of one retry pass: the columns of one mainnet block.
	maxPendingGloasVerificationsPerPass = 128
)

var (
	errGloasSidecarBlockUnavailable = fmt.Errorf("%w: data column sidecar block unavailable", ErrIgnore)
	errInvalidGloasSidecar          = errors.New("data column sidecar proven invalid against its block")
	errGloasVerificationBudget      = errors.New("pending data column sidecar verification budget exhausted")

	verifyDataColumnSidecarInclusionProof           = das.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs                = das.VerifyDataColumnSidecarKZGProofs
	verifyDataColumnSidecar                         = das.VerifyDataColumnSidecar
	verifyDataColumnSidecarWithCommitments          = das.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = das.VerifyDataColumnSidecarKZGProofsWithCommitments
	computeSubnetForDataColumnSidecar               = das.ComputeSubnetForDataColumnSidecar
)

// pendingGloasSidecar is a queued candidate and the peer that forwarded it. An empty peer is exempt from the quota.
type pendingGloasSidecar struct {
	sidecar *cltypes.DataColumnSidecar
	source  pendingSidecarSource
}

type pendingSidecarSource struct {
	peer   peer.ID
	subnet uint64
}

func (s pendingSidecarSource) known() bool { return s.peer != "" }

type pendingSidecarQuota struct {
	mu    sync.Mutex
	limit int
	used  map[pendingSidecarSource]int
}

func (q *pendingSidecarQuota) acquire(source pendingSidecarSource) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.used[source] >= q.limit {
		return false
	}
	q.used[source]++
	return true
}

func (q *pendingSidecarQuota) release(source pendingSidecarSource) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.used[source] <= 1 {
		delete(q.used, source)
		return
	}
	q.used[source]--
}

// pendingGloasSidecarQuota is the number of slots a candidate can stay queued, plus one for a candidate that arrives
// as the previous one of the same source expires.
func pendingGloasSidecarQuota(cfg *clparams.BeaconChainConfig) int {
	slot := time.Duration(max(cfg.SecondsPerSlot, 1)) * time.Second
	return int((pendingGloasSidecarExpiry+slot-1)/slot) + 1
}

type dataColumnSidecarService struct {
	cfg                  *clparams.BeaconChainConfig
	ethClock             eth_clock.EthereumClock
	forkChoice           forkchoice.ForkChoiceStorage
	syncDataManager      synced_data.SyncedData
	seenSidecar          *lru.Cache[seenSidecarKey, struct{}]
	seenGloas            *seenGloasSidecars
	columnSidecarStorage blob_storage.DataColumnStorage
	emitters             *beaconevents.EventEmitter
	gossipManager        gossipmgr.Gossip
	expectedProposer     func(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error)
	ctx                  context.Context
	backgroundWrites     chan struct{}

	// Key by the full sidecar root so an unverified candidate cannot suppress
	// another candidate for the same block and column.
	pendingGloasSidecars *pendingJobQueue[common.Hash, pendingGloasSidecar]
	pendingQuota         pendingSidecarQuota
	// verificationsLeft and bannedInPass are only used by the queue's processing goroutine.
	verificationsLeft int
	bannedInPass      map[peer.ID]struct{}
}

// seenSidecarKey is used for Fulu (pre-GLOAS) seen tracking
type seenSidecarKey struct {
	slot          uint64
	proposerIndex uint64
	index         uint64
}

// seenGloasSidecarKey is used for GLOAS seen tracking
// [New in Gloas:EIP7732] The sidecar is the first sidecar for the tuple (sidecar.beacon_block_root, sidecar.index)
type seenGloasSidecarKey struct {
	beaconBlockRoot common.Hash
	index           uint64
}

// seenGloasSidecars marks Gloas sidecar keys as seen. A release removes a mark only while its token is still current.
type seenGloasSidecars struct {
	mu    sync.Mutex
	cache *lru.Cache[seenGloasSidecarKey, uint64]
	next  uint64
}

func newSeenGloasSidecars(metricName string, size int) *seenGloasSidecars {
	cache, err := lru.New[seenGloasSidecarKey, uint64](metricName, size)
	if err != nil {
		panic(err)
	}
	return &seenGloasSidecars{cache: cache}
}

func (s *seenGloasSidecars) contains(k seenGloasSidecarKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.cache.Get(k)
	return ok
}

// claim marks k as seen unless it is. It returns a function that removes the mark it took.
func (s *seenGloasSidecars) claim(k seenGloasSidecarKey) (release func(), claimed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache.Contains(k) {
		return nil, false
	}
	s.next++
	token := s.next
	s.cache.Add(k, token)
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if current, ok := s.cache.Peek(k); ok && current == token {
			s.cache.Remove(k)
		}
	}, true
}

func NewDataColumnSidecarService(
	ctx context.Context,
	cfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	forkChoice forkchoice.ForkChoiceStorage,
	syncDataManager synced_data.SyncedData,
	columnSidecarStorage blob_storage.DataColumnStorage,
	emitters *beaconevents.EventEmitter,
	gossipManager gossipmgr.Gossip,
) DataColumnSidecarService {
	size := cfg.NumberOfColumns * cfg.SlotsPerEpoch * 4
	seenSidecar, err := lru.New[seenSidecarKey, struct{}]("seenDataColumnSidecar", int(size))
	if err != nil {
		panic(err)
	}
	s := &dataColumnSidecarService{
		cfg:                  cfg,
		ethClock:             ethClock,
		forkChoice:           forkChoice,
		syncDataManager:      syncDataManager,
		seenSidecar:          seenSidecar,
		seenGloas:            newSeenGloasSidecars("seenGloasDataColumnSidecar", int(size)),
		columnSidecarStorage: columnSidecarStorage,
		emitters:             emitters,
		gossipManager:        gossipManager,
		ctx:                  ctx,
		backgroundWrites:     make(chan struct{}, cfg.NumberOfColumns),
		pendingQuota: pendingSidecarQuota{
			limit: pendingGloasSidecarQuota(cfg),
			used:  make(map[pendingSidecarSource]int),
		},
		bannedInPass: make(map[peer.ID]struct{}),
	}
	s.expectedProposer = newExpectedProposers(ctx, cfg, forkChoice, syncDataManager).get
	s.pendingGloasSidecars = s.newPendingGloasSidecarQueue(ctx)
	return s
}

func (s *dataColumnSidecarService) newPendingGloasSidecarQueue(ctx context.Context) *pendingJobQueue[common.Hash, pendingGloasSidecar] {
	return newPendingJobQueue(ctx, pendingJobQueueOptions{
		name:          "gloas_data_column_sidecar",
		capacity:      maxPendingGloasSidecars,
		expiry:        pendingGloasSidecarExpiry,
		checkInterval: pendingGloasSidecarTick,
	},
		s.tryProcessPendingGloasSidecar,
		nil,
		func(_ common.Hash, pending pendingGloasSidecar) {
			sidecar := pending.sidecar
			log.Debug("[dataColumnSidecarService] expired pending GLOAS sidecar",
				"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index)
		},
		func(q *pendingJobQueue[common.Hash, pendingGloasSidecar]) {
			q.beforePass = func() {
				s.verificationsLeft = maxPendingGloasVerificationsPerPass
				clear(s.bannedInPass)
			}
			q.onRemoved = func(_ common.Hash, pending pendingGloasSidecar) {
				if pending.source.known() {
					s.pendingQuota.release(pending.source)
				}
			}
		})
}

func (s *dataColumnSidecarService) Names() []string {
	names := make([]string, 0, s.cfg.DataColumnSidecarSubnetCount)
	for i := 0; i < int(s.cfg.DataColumnSidecarSubnetCount); i++ {
		names = append(names, gossip.TopicNameDataColumnSidecar(uint64(i)))
	}
	return names
}

func (s *dataColumnSidecarService) IsMyGossipMessage(name string) bool {
	return gossip.IsTopicDataColumnSidecar(name)
}

func (s *dataColumnSidecarService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.DataColumnSidecar, error) {
	obj := cltypes.NewDataColumnSidecarWithVersion(version)
	if err := decodeGossipSSZ(obj, data, version); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *dataColumnSidecarService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	if s.syncDataManager.Syncing() {
		return ErrIgnore
	}

	// Version-aware processing
	if msg.Version() >= clparams.GloasVersion {
		err := s.processGloasMessage(ctx, subnet, msg, false)
		if errors.Is(err, errGloasSidecarBlockUnavailable) {
			s.scheduleSidecarForLaterProcessing(ctx, msg)
		}
		return err
	}
	return s.processFuluMessage(ctx, subnet, msg)
}

// processFuluMessage handles Fulu (pre-GLOAS) data column sidecar validation
// Reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/fulu/p2p-interface.md
func (s *dataColumnSidecarService) processFuluMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	if msg.SignedBlockHeader == nil || msg.SignedBlockHeader.Header == nil {
		return errors.New("missing signed block header for fulu sidecar")
	}

	blockHeader := msg.SignedBlockHeader.Header
	if msg.Version() != s.cfg.GetCurrentStateVersion(blockHeader.Slot/s.cfg.SlotsPerEpoch) {
		return fmt.Errorf("%w: data column sidecar schema does not match slot", ErrIgnore)
	}
	seenKey := seenSidecarKey{
		slot:          blockHeader.Slot,
		proposerIndex: blockHeader.ProposerIndex,
		index:         msg.Index,
	}

	// [IGNORE] The sidecar is the first sidecar for the tuple (block_header.slot, block_header.proposer_index, sidecar.index)
	if _, ok := s.seenSidecar.Get(seenKey); ok {
		return ErrIgnore
	}

	blockRoot, err := blockHeader.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to get block root: %w", err)
	}

	store, err := s.shouldStore(blockHeader.Slot, blockRoot, msg.Index)
	if err != nil {
		return err
	}

	blobParameters := s.cfg.GetBlobParameters(blockHeader.Slot / s.cfg.SlotsPerEpoch)
	if msg.Column.Len() > int(blobParameters.MaxBlobsPerBlock) {
		log.Warn("invalid column sidecar length", "blockRoot", blockRoot, "columnIndex", msg.Index, "columnLen", msg.Column.Len())
		return errors.New("invalid column sidecar length")
	}

	// [REJECT] The sidecar is valid as verified by verify_data_column_sidecar(sidecar).
	if !verifyDataColumnSidecar(msg) {
		return errors.New("invalid data column sidecar")
	}

	// [REJECT] The sidecar is for the correct subnet
	if subnet != nil && *subnet != computeSubnetForDataColumnSidecar(msg.Index) {
		return fmt.Errorf("incorrect subnet %d for data column sidecar index %d", *subnet, msg.Index)
	}

	// [IGNORE] The sidecar is not from a future slot
	if blockHeader.Slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(blockHeader.Slot) {
		return ErrIgnore
	}

	// One finalized checkpoint for both finality checks below, so that a checkpoint update between
	// them cannot turn an ignored sidecar into a rejected one.
	finalizedCheckpoint := s.forkChoice.FinalizedCheckpoint()
	// [IGNORE] The sidecar is from a slot greater than the latest finalized slot -- i.e. validate that
	// block_header.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch).
	finalizedStartSlot, ok := safeMultiplyUint64(finalizedCheckpoint.Epoch, s.cfg.SlotsPerEpoch)
	if !ok {
		return errors.New("finalized checkpoint slot is not representable")
	}
	if blockHeader.Slot <= finalizedStartSlot {
		return ErrIgnore
	}

	// [REJECT] The proposer signature is valid
	if pass, err := s.verifyProposerSignature(blockHeader.ProposerIndex, msg.SignedBlockHeader); err != nil {
		return fmt.Errorf("invalid proposer signature for data column sidecar: %w", err)
	} else if !pass {
		return errors.New("invalid proposer signature for data column sidecar")
	}

	// [IGNORE] The sidecar's block's parent has been seen
	parentHeader, ok := s.forkChoice.GetHeader(blockHeader.ParentRoot)
	if !ok {
		return ErrIgnore
	}

	// [REJECT] The sidecar is from a higher slot than parent
	if blockHeader.Slot <= parentHeader.Slot {
		return fmt.Errorf("data column sidecar should be from a higher slot than the parent block, but got %d <= %d", blockHeader.Slot, parentHeader.Slot)
	}

	// [REJECT] The finalized checkpoint is an ancestor, clamped to the anchor slot.
	if s.forkChoice.Ancestor(blockHeader.ParentRoot, max(finalizedStartSlot, s.forkChoice.AnchorSlot())).Root != finalizedCheckpoint.Root {
		return errors.New("finalized checkpoint is not an ancestor of the sidecar's block")
	}

	// [REJECT] The inclusion proof is valid
	if !verifyDataColumnSidecarInclusionProof(msg) {
		return errors.New("invalid inclusion proof for data column sidecar")
	}

	// [REJECT] The KZG proofs are valid
	if !verifyDataColumnSidecarKZGProofs(msg) {
		return errors.New("invalid kzg proofs for data column sidecar")
	}

	// [REJECT] The sidecar is proposed by the expected proposer_index for the block's slot in the context of the
	// current shuffling. It comes after the proof checks, as in the spec, so an invalid sidecar is rejected even when
	// the proposer read ignores it. A block in fork choice passed the state transition, which checks its proposer.
	if _, known := s.forkChoice.GetHeader(blockRoot); !known {
		expectedProposer, err := s.expectedProposer(ctx, blockHeader.ParentRoot, parentHeader.Slot, blockHeader.Slot)
		if err != nil {
			return err
		}
		if blockHeader.ProposerIndex != expectedProposer {
			return fmt.Errorf("data column sidecar proposer index %d does not match expected proposer %d", blockHeader.ProposerIndex, expectedProposer)
		}
	}

	// Claim the tuple atomically: columns built from EL blobs may have claimed it during validation.
	if seen, _ := s.seenSidecar.ContainsOrAdd(seenKey, struct{}{}); seen {
		return ErrIgnore
	}
	msg.BlockRoot = blockRoot
	msg.Slot = blockHeader.Slot
	s.emitters.Operation().SendDataColumnSidecar(beaconevents.NewDataColumnSidecarData(msg))
	if !store {
		return nil
	}
	select {
	case s.backgroundWrites <- struct{}{}:
		go func() {
			defer func() { <-s.backgroundWrites }()
			if err := s.storeFuluSidecar(seenKey, blockRoot, msg); err != nil {
				log.Debug("[dataColumnSidecarService] failed to store accepted data column sidecar", "slot", blockHeader.Slot, "blockRoot", common.Hash(blockRoot).String(), "index", msg.Index, "err", err)
			}
		}()
		return nil
	default:
		return s.storeFuluSidecar(seenKey, blockRoot, msg)
	}
}

// storeFuluSidecar writes a column whose data_column_sidecar event is already sent. A failed write releases the seen
// key so the column can be fetched again.
func (s *dataColumnSidecarService) storeFuluSidecar(seenKey seenSidecarKey, blockRoot common.Hash, msg *cltypes.DataColumnSidecar) error {
	slot := msg.Slot
	if err := s.columnSidecarStorage.WriteColumnSidecarsWithoutEvent(s.ctx, blockRoot, int64(msg.Index), msg); err != nil {
		s.seenSidecar.Remove(seenKey)
		return fmt.Errorf("%w: failed to write data column sidecar: %w", ErrIgnore, err)
	}

	if err := s.forkChoice.GetPeerDas().TryScheduleRecover(slot, blockRoot); err != nil {
		log.Warn("failed to schedule recover", "err", err, "slot", slot, "blockRoot", blockRoot.String())
	}
	log.Trace("[dataColumnSidecarService] processed fulu data column sidecar", "slot", slot, "blockRoot", blockRoot.String(), "index", msg.Index)
	return nil
}

// shouldStore reports whether the node stores a column it has validated. A column it does not store is still relayed
// on a subscribed subnet. An archive node also stores columns outside custody until it can recover, or has recovered,
// the blobs.
func (s *dataColumnSidecarService) shouldStore(slot uint64, blockRoot common.Hash, index uint64) (bool, error) {
	peerDas := s.forkChoice.GetPeerDas()
	myCustodyColumns, err := peerDas.StateReader().GetMyCustodyColumns()
	if err != nil {
		return false, fmt.Errorf("%w: failed to get my custody columns: %w", ErrIgnore, err)
	}
	if _, inCustody := myCustodyColumns[index]; inCustody {
		return true, nil
	}
	return peerDas.IsArchivedMode() && !peerDas.IsColumnOverHalf(slot, blockRoot) && !peerDas.IsBlobAlreadyRecovered(blockRoot), nil
}

// PublishColumns marks each sidecar of one block as seen and publishes it on its subnet, unless the sidecar was seen
// already. The sidecars must be on subnets this node subscribes to.
func (s *dataColumnSidecarService) PublishColumns(ctx context.Context, sidecars []*cltypes.DataColumnSidecar) {
	s.ClaimColumns(sidecars)(ctx)
}

// ClaimColumns marks the unseen sidecars of one block as seen. The returned function publishes the claimed ones and
// releases the mark of each one it fails to publish.
func (s *dataColumnSidecarService) ClaimColumns(sidecars []*cltypes.DataColumnSidecar) (publish func(context.Context)) {
	if len(sidecars) == 0 {
		return func(context.Context) {}
	}
	// The block may have waited for its parent, the EL or recovery while finality moved on; peers would ignore
	// or reject its Fulu columns then.
	slot, ok := s.afterFinality(sidecars[0])
	if !ok {
		log.Debug("[dataColumnSidecarService] not publishing local columns below finality", "slot", slot)
		return func(context.Context) {}
	}
	forkDigest, err := s.ethClock.ComputeForkDigest(slot / s.cfg.SlotsPerEpoch)
	if err != nil {
		log.Warn("[dataColumnSidecarService] failed to compute the fork digest of local columns", "slot", slot, "err", err)
		return func(context.Context) {}
	}
	var claimed []*cltypes.DataColumnSidecar
	var releases []func()
	for _, sidecar := range sidecars {
		if release, ok := s.claimSeen(sidecar); ok {
			claimed = append(claimed, sidecar)
			releases = append(releases, release)
		}
	}
	return func(ctx context.Context) {
		for i, sidecar := range claimed {
			encoded, err := sidecar.EncodeSSZ(nil)
			if err == nil {
				err = s.gossipManager.PublishToForkDigest(ctx, forkDigest, gossip.TopicNameDataColumnSidecar(computeSubnetForDataColumnSidecar(sidecar.Index)), encoded)
			}
			if err != nil {
				// Unclaimed, a copy from gossip is still processed and relayed.
				releases[i]()
				log.Debug("[dataColumnSidecarService] failed to publish a local column", "slot", slot, "index", sidecar.Index, "err", err)
			}
		}
	}
}

// afterFinality returns the sidecar's slot and whether peers would still accept the sidecar. For Fulu, its slot is
// after the finalized epoch start and the finalized checkpoint is an ancestor of its block. Gloas has no finality
// rules, but its block must be imported or still importable: recovery also runs for blocks backfilled below finality,
// whose columns peers no longer need.
func (s *dataColumnSidecarService) afterFinality(sidecar *cltypes.DataColumnSidecar) (uint64, bool) {
	if sidecar.Version() >= clparams.GloasVersion {
		_, _, notImportable := s.lookupBlock(sidecar.Slot, sidecar.BeaconBlockRoot)
		return sidecar.Slot, !notImportable
	}
	header := sidecar.SignedBlockHeader.Header
	finalizedCheckpoint := s.forkChoice.FinalizedCheckpoint()
	finalizedStartSlot, ok := safeMultiplyUint64(finalizedCheckpoint.Epoch, s.cfg.SlotsPerEpoch)
	if !ok || header.Slot <= finalizedStartSlot {
		return header.Slot, false
	}
	return header.Slot, s.forkChoice.Ancestor(header.ParentRoot, max(finalizedStartSlot, s.forkChoice.AnchorSlot())).Root == finalizedCheckpoint.Root
}

// claimSeen marks the sidecar as seen. It reports whether the sidecar was not seen before, and returns a function that
// removes the mark.
func (s *dataColumnSidecarService) claimSeen(sidecar *cltypes.DataColumnSidecar) (release func(), claimed bool) {
	if sidecar.Version() >= clparams.GloasVersion {
		key := seenGloasSidecarKey{beaconBlockRoot: sidecar.BeaconBlockRoot, index: sidecar.Index}
		return s.seenGloas.claim(key)
	}
	header := sidecar.SignedBlockHeader.Header
	key := seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: sidecar.Index}
	seen, _ := s.seenSidecar.ContainsOrAdd(key, struct{}{})
	return func() { s.seenSidecar.Remove(key) }, !seen
}

// processGloasMessage handles GLOAS data column sidecar validation
// Reference: https://github.com/ethereum/consensus-specs/blob/master/specs/gloas/p2p-interface.md
func (s *dataColumnSidecarService) processGloasMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar, deferred bool) error {
	slot := msg.Slot
	if msg.Version() != s.cfg.GetCurrentStateVersion(slot/s.cfg.SlotsPerEpoch) {
		return fmt.Errorf("%w: data column sidecar schema does not match slot", ErrIgnore)
	}
	blockRoot := msg.BeaconBlockRoot

	seenKey := seenGloasSidecarKey{
		beaconBlockRoot: blockRoot,
		index:           msg.Index,
	}

	// [IGNORE] The sidecar is the first sidecar for the tuple (sidecar.beacon_block_root, sidecar.index)
	if s.seenGloas.contains(seenKey) {
		return ErrIgnore
	}

	// [REJECT] Validate the subnet before queue admission so a wrong-subnet copy
	// cannot suppress a later copy received on the correct subnet.
	if subnet != nil && *subnet != computeSubnetForDataColumnSidecar(msg.Index) {
		return fmt.Errorf("incorrect subnet %d for data column sidecar index %d", *subnet, msg.Index)
	}

	// [IGNORE] The sidecar is not from a future slot (with some tolerance for clock disparity)
	if slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(slot) {
		return ErrIgnore
	}

	store, err := s.shouldStore(slot, blockRoot, msg.Index)
	if err != nil {
		return err
	}

	// [IGNORE] A valid block for the sidecar's slot has been seen.
	// Only checks recent blocks in forkChoice memory - older blocks don't need sidecar validation.
	block, ok, notImportable := s.lookupBlock(slot, blockRoot)
	if !ok {
		// [REJECT] The block for the sidecar passes validation.
		if s.forkChoice.IsBlockInvalid(blockRoot) {
			return fmt.Errorf("%w: block %v failed validation", errInvalidGloasSidecar, blockRoot)
		}
		if !store || notImportable {
			return ErrIgnore
		}
		return errGloasSidecarBlockUnavailable
	}

	// [REJECT] The sidecar's slot matches the slot of the block
	if slot != block.Block.Slot {
		return fmt.Errorf("%w: sidecar slot %d does not match block slot %d", errInvalidGloasSidecar, slot, block.Block.Slot)
	}

	// Get kzg_commitments from bid = block.body.signed_execution_payload_bid.message
	if block.Block.Body.SignedExecutionPayloadBid == nil ||
		block.Block.Body.SignedExecutionPayloadBid.Message == nil {
		return errors.New("block does not have SignedExecutionPayloadBid")
	}
	kzgCommitments := &block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments

	blobParameters := s.cfg.GetBlobParameters(slot / s.cfg.SlotsPerEpoch)
	if msg.Column.Len() > int(blobParameters.MaxBlobsPerBlock) {
		log.Warn("invalid column sidecar length", "blockRoot", blockRoot, "columnIndex", msg.Index, "columnLen", msg.Column.Len())
		return fmt.Errorf("%w: invalid column sidecar length", errInvalidGloasSidecar)
	}

	// [REJECT] The sidecar is valid as verified by verify_data_column_sidecar(sidecar, bid.blob_kzg_commitments)
	if !verifyDataColumnSidecarWithCommitments(msg, kzgCommitments) {
		return fmt.Errorf("%w: invalid data column sidecar", errInvalidGloasSidecar)
	}

	if deferred {
		if s.verificationsLeft <= 0 {
			return errGloasVerificationBudget
		}
		s.verificationsLeft--
	}

	// [REJECT] The sidecar's column data is valid as verified by verify_data_column_sidecar_kzg_proofs(sidecar, bid.blob_kzg_commitments)
	if !verifyDataColumnSidecarKZGProofsWithCommitments(msg, kzgCommitments) {
		return fmt.Errorf("%w: invalid kzg proofs for data column sidecar", errInvalidGloasSidecar)
	}

	// Another writer may have claimed the key during validation.
	release, claimed := s.seenGloas.claim(seenKey)
	if !claimed {
		return ErrIgnore
	}
	if !store {
		msg.BlockRoot = blockRoot
		s.emitters.Operation().SendDataColumnSidecar(beaconevents.NewDataColumnSidecarData(msg))
		return nil
	}
	written := false
	defer func() {
		if !written {
			release()
		}
	}()
	if err := s.columnSidecarStorage.WriteColumnSidecars(ctx, blockRoot, int64(msg.Index), msg); err != nil {
		return fmt.Errorf("%w: failed to write data column sidecar: %w", ErrIgnore, err)
	}
	written = true

	if err := s.forkChoice.GetPeerDas().TryScheduleRecover(slot, blockRoot); err != nil {
		log.Warn("failed to schedule recover", "err", err, "slot", slot, "blockRoot", blockRoot.String())
	}
	log.Trace("[dataColumnSidecarService] processed gloas data column sidecar", "slot", slot, "blockRoot", blockRoot.String(), "index", msg.Index)
	return nil
}

func (s *dataColumnSidecarService) verifyProposerSignature(proposerIndex uint64, signedBlockHeader *cltypes.SignedBeaconBlockHeader) (bool, error) {
	var (
		valid       bool
		pk          common.Bytes48
		signingRoot common.Hash
	)
	err := s.syncDataManager.ViewHeadState(func(state *st.CachingBeaconState) error {
		proposer, err := state.ValidatorForValidatorIndex(int(proposerIndex))
		if err != nil {
			return fmt.Errorf("unable to retrieve state: %w", err)
		}

		domain, err := fork.ComputeDomainAtEpoch(s.cfg, s.cfg.DomainBeaconProposer, st.GetEpochAtSlot(s.cfg, signedBlockHeader.Header.Slot), state.GenesisValidatorsRoot())
		if err != nil {
			return fmt.Errorf("unable to get domain: %w", err)
		}
		pk = proposer.PublicKey()
		signingRoot, err = computeSigningRoot(signedBlockHeader.Header, domain)
		if err != nil {
			return fmt.Errorf("unable to compute signing root: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	valid, err = blsVerify(signedBlockHeader.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return false, fmt.Errorf("unable to verify signature: %w", err)
	}
	return valid, nil
}

// lookupBlock returns the block of root once fork choice has imported it, else whether it can never be imported.
// Fork choice holds a block while its state transition runs, and publishes the header only after the transition
// succeeds. The importability sample waits for an import in progress and precedes the last lookup, so that lookup
// sees a block imported meanwhile, and a miss after a "not importable" sample is final.
func (s *dataColumnSidecarService) lookupBlock(slot uint64, root common.Hash) (block *cltypes.SignedBeaconBlock, ok, notImportable bool) {
	if block, ok = s.importedBlock(root); ok {
		return block, true, false
	}
	notImportable = s.blockNotImportable(slot)
	block, ok = s.importedBlock(root)
	return block, ok, !ok && notImportable
}

func (s *dataColumnSidecarService) importedBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if _, ok := s.forkChoice.GetHeader(root); !ok {
		return nil, false
	}
	return s.forkChoice.GetBlock(root)
}

// blockNotImportable reports whether fork choice rejects a block at slot, as at or below the finalized checkpoint
// start slot or the anchor slot. A Gloas sidecar whose block is unknown at such a slot can never be validated.
func (s *dataColumnSidecarService) blockNotImportable(slot uint64) bool {
	finalizedStartSlot, ok := safeMultiplyUint64(s.forkChoice.FinalizedCheckpoint().Epoch, s.cfg.SlotsPerEpoch)
	return !ok || slot <= max(finalizedStartSlot, s.forkChoice.AnchorSlot())
}

// scheduleSidecarForLaterProcessing queues a GLOAS sidecar until its block arrives.
func (s *dataColumnSidecarService) scheduleSidecarForLaterProcessing(ctx context.Context, sidecar *cltypes.DataColumnSidecar) {
	// Bound hashing and retained data before the block's commitments are available.
	// Failed admission keeps the missing-block IGNORE verdict from gossip validation.
	blobParameters := s.cfg.GetBlobParameters(sidecar.Slot / s.cfg.SlotsPerEpoch)
	if !verifyDataColumnSidecar(sidecar) || sidecar.Column.Len() > int(blobParameters.MaxBlobsPerBlock) {
		return
	}
	pending := pendingGloasSidecar{sidecar: sidecar}
	if forwarder, ok := serviceinterface.GossipPeer(ctx); ok {
		pending.source = pendingSidecarSource{peer: forwarder, subnet: computeSubnetForDataColumnSidecar(sidecar.Index)}
		if !s.pendingQuota.acquire(pending.source) {
			log.Trace("[dataColumnSidecarService] pending GLOAS sidecar peer quota reached, dropping",
				"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index, "peer", forwarder)
			return
		}
	}
	inserted := false
	defer func() {
		if !inserted && pending.source.known() {
			s.pendingQuota.release(pending.source)
		}
	}()
	inserted, err := s.pendingGloasSidecars.enqueueLazy(pending, func() (common.Hash, error) { return sidecar.HashSSZ() })
	if errors.Is(err, errPendingJobQueueFull) {
		log.Trace("[dataColumnSidecarService] pending GLOAS sidecars at capacity, dropping",
			"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index)
	} else if err != nil {
		log.Warn("[dataColumnSidecarService] failed to hash pending GLOAS sidecar",
			"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index, "err", err)
	}
}

func (s *dataColumnSidecarService) tryProcessPendingGloasSidecar(
	ctx context.Context,
	_ common.Hash,
	pending pendingGloasSidecar,
) pendingJobDecision {
	sidecar := pending.sidecar
	if _, ok, notImportable := s.lookupBlock(sidecar.Slot, sidecar.BeaconBlockRoot); !ok && !s.forkChoice.IsBlockInvalid(sidecar.BeaconBlockRoot) {
		if notImportable {
			log.Debug("[dataColumnSidecarService] pending GLOAS sidecar's block can no longer be imported",
				"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index)
			return pendingJobRemove
		}
		return pendingJobKeep
	}
	// Subnet validation already ran before queue admission.
	if err := s.processGloasMessage(ctx, nil, sidecar, true); err != nil {
		if errors.Is(err, errGloasSidecarBlockUnavailable) || errors.Is(err, errGloasVerificationBudget) {
			return pendingJobKeep
		}
		if errors.Is(err, errInvalidGloasSidecar) && pending.source.known() {
			if _, banned := s.bannedInPass[pending.source.peer]; !banned {
				s.bannedInPass[pending.source.peer] = struct{}{}
				s.gossipManager.BanPeer(pending.source.peer)
				log.Debug("[dataColumnSidecarService] banned peer for an invalid pending GLOAS sidecar",
					"peer", pending.source.peer, "slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index, "err", err)
			}
			return pendingJobRemove
		}
		if !errors.Is(err, ErrIgnore) {
			log.Trace("[dataColumnSidecarService] failed to process pending GLOAS sidecar",
				"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index, "err", err)
		}
		return pendingJobRemove
	}
	log.Debug("[dataColumnSidecarService] successfully processed pending GLOAS sidecar",
		"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index)
	return pendingJobRemove
}
