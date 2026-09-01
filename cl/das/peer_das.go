package das

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	gossipmgr "github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/p2p/enode"
)

// BlockGetter is an interface for getting blocks by root.
// Used to avoid import cycle with forkchoice package.
// [New in Gloas:EIP7732]
type BlockGetter interface {
	GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool)
}

// gloasBlockData holds only the fields needed from a block for GLOAS sidecar verification.
// This is much smaller than caching the full SignedBeaconBlock (~1KB vs ~100KB-2MB).
// [New in Gloas:EIP7732]
type gloasBlockData struct {
	BlobKzgCommitments      *solid.ListSSZ[*cltypes.KZGCommitment]
	SignedBeaconBlockHeader *cltypes.SignedBeaconBlockHeader
}

//go:generate mockgen -typed=true -destination=mock_services/peer_das_mock.go -package=mock_services . PeerDas
type PeerDas interface {
	Start(ctx context.Context)
	// [Modified in Gloas:EIP7732] Changed from []*SignedBlindedBeaconBlock to []ColumnSyncableSignedBlock
	// to support both pre-GLOAS (blinded) and GLOAS (non-blinded) blocks
	DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error
	DownloadOnlyCustodyColumns(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error
	IsDataAvailable(slot uint64, blockRoot common.Hash) (bool, error)
	PruneBelow(slot uint64) error
	UpdateValidatorsCustody(cgc uint64)
	TryScheduleRecover(slot uint64, blockRoot common.Hash) error
	IsBlobAlreadyRecovered(blockRoot common.Hash) bool
	IsColumnOverHalf(slot uint64, blockRoot common.Hash) bool
	IsArchivedMode() bool
	StateReader() peerdasstate.PeerDasStateReader
	SyncColumnDataLater(block *cltypes.SignedBeaconBlock) error
	SetForkChoice(forkChoice BlockGetter) // [New in Gloas:EIP7732]
}

var numOfBlobRecoveryWorkers = 8

const (
	blobRecoveryValidationRetryInterval = 500 * time.Millisecond
	maxBlobRecoveryResultBytes          = 16 << 20
)

type peerdas struct {
	state             *peerdasstate.PeerDasState
	nodeID            enode.ID
	rpc               *rpc.BeaconRpcP2P
	beaconConfig      *clparams.BeaconChainConfig
	caplinConfig      *clparams.CaplinConfig
	columnStorage     blob_storage.DataColumnStorage
	blobStorage       blob_storage.BlobStorage
	sentinel          sentinelproto.SentinelClient
	ethClock          eth_clock.EthereumClock
	gossipManager     gossipmgr.Gossip
	recoverBlobsQueue chan recoverBlobsRequest

	recoveringMutex     sync.Mutex
	isRecovering        map[common.Hash]bool
	recoveryRequests    map[common.Hash]recoverBlobsRequest
	recoveryGenerations map[common.Hash]uint64
	recoveryResults     map[common.Hash]*blobRecoveryResult
	recoveryResultOrder []common.Hash
	recoveryResultBytes int
	recoveryRetryOnce   sync.Once
	recoveryRetryMutex  sync.Mutex
	recoveryRetries     map[common.Hash]*delayedRecoverBlobsRequest
	recoveryRetryQueue  blobRecoveryRetryHeap
	recoveryRetryAfter  time.Time
	recoveryRetryWake   chan struct{}
	recoveryPreferRetry bool
	recoverySlots       map[uint64]*blobRecoverySlot
	recoverySlotQueue   blobRecoverySlotHeap
	recoveryPruneFloor  uint64
	blocksToCheckSync   sync.Map // blockRoot -> ColumnSyncableSignedBlock (SignedBeaconBlock or SignedBlindedBeaconBlock)

	// [New in Gloas:EIP7732] For fetching blocks to get kzg_commitments
	forkChoice     BlockGetter
	blockReader    freezeblocks.BeaconSnapshotReader
	indiciesDB     kv.RoDB
	gloasDataCache *lru.Cache[common.Hash, *gloasBlockData] // cache for GLOAS block data (~1KB per entry)
	startOnce      sync.Once
}

func NewPeerDas(
	rpc *rpc.BeaconRpcP2P,
	beaconConfig *clparams.BeaconChainConfig,
	caplinConfig *clparams.CaplinConfig,
	columnStorage blob_storage.DataColumnStorage,
	blobStorage blob_storage.BlobStorage,
	sentinel sentinelproto.SentinelClient,
	nodeID enode.ID,
	ethClock eth_clock.EthereumClock,
	peerDasState *peerdasstate.PeerDasState,
	gossipManager gossipmgr.Gossip,
	blockReader freezeblocks.BeaconSnapshotReader, // [New in Gloas:EIP7732]
	indiciesDB kv.RoDB, // [New in Gloas:EIP7732]
) PeerDas {
	kzg.InitKZGCtx()
	gloasDataCache, _ := lru.New[common.Hash, *gloasBlockData]("gloasDataCache", 128)
	p := &peerdas{
		state:             peerDasState,
		nodeID:            nodeID,
		rpc:               rpc,
		beaconConfig:      beaconConfig,
		caplinConfig:      caplinConfig,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		sentinel:          sentinel,
		ethClock:          ethClock,
		gossipManager:     gossipManager,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 128),

		recoveringMutex:     sync.Mutex{},
		isRecovering:        make(map[common.Hash]bool),
		recoveryRequests:    make(map[common.Hash]recoverBlobsRequest),
		recoveryGenerations: make(map[common.Hash]uint64),
		recoveryResults:     make(map[common.Hash]*blobRecoveryResult),
		recoveryRetries:     make(map[common.Hash]*delayedRecoverBlobsRequest),
		recoveryRetryWake:   make(chan struct{}, numOfBlobRecoveryWorkers),
		blocksToCheckSync:   sync.Map{},

		blockReader:    blockReader,
		indiciesDB:     indiciesDB,
		gloasDataCache: gloasDataCache,
	}
	return p
}

func (d *peerdas) Start(ctx context.Context) {
	d.startOnce.Do(func() {
		d.resubscribeGossip()
		for range numOfBlobRecoveryWorkers {
			go d.blobsRecoverWorker(ctx)
		}
		go d.syncColumnDataWorker(ctx)
	})
}

func (d *peerdas) StateReader() peerdasstate.PeerDasStateReader {
	return d.state
}

// SetForkChoice sets the fork choice storage reader.
// This is called after forkChoice is initialized to avoid circular dependency.
// [New in Gloas:EIP7732]
func (d *peerdas) SetForkChoice(forkChoice BlockGetter) {
	d.forkChoice = forkChoice
}

// getGloasData retrieves the GLOAS block data (kzg_commitments and SignedBlockHeader) for sidecar verification.
// Uses LRU cache (~1KB per entry), tries forkChoice first (recent blocks), then falls back to blockReader (historical blocks).
// [New in Gloas:EIP7732]
func (d *peerdas) getGloasData(blockRoot common.Hash) (*gloasBlockData, error) {
	// Check cache first
	if data, ok := d.gloasDataCache.Get(blockRoot); ok {
		return data, nil
	}

	// Try forkChoice first (in-memory recent blocks)
	if d.forkChoice != nil {
		if block, ok := d.forkChoice.GetBlock(blockRoot); ok {
			data := d.extractGloasData(block)
			if data != nil {
				d.gloasDataCache.Add(blockRoot, data)
			}
			return data, nil
		}
	}

	// Fall back to blockReader for historical blocks
	if d.blockReader != nil && d.indiciesDB != nil {
		tx, err := d.indiciesDB.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		block, err := d.blockReader.ReadBlockByRoot(context.Background(), tx, blockRoot)
		if err != nil {
			return nil, err
		}
		if block != nil {
			data := d.extractGloasData(block)
			if data != nil {
				d.gloasDataCache.Add(blockRoot, data)
			}
			return data, nil
		}
	}

	return nil, errors.New("block not found for GLOAS")
}

// extractGloasData extracts the needed fields from a SignedBeaconBlock for GLOAS.
// [New in Gloas:EIP7732]
func (d *peerdas) extractGloasData(block *cltypes.SignedBeaconBlock) *gloasBlockData {
	if block == nil {
		return nil
	}
	bid := block.Block.Body.SignedExecutionPayloadBid
	if bid == nil || bid.Message == nil {
		return nil
	}
	return &gloasBlockData{
		BlobKzgCommitments:      &bid.Message.BlobKzgCommitments,
		SignedBeaconBlockHeader: block.SignedBeaconBlockHeader(),
	}
}

// getKzgCommitmentsForGloas retrieves kzg_commitments for GLOAS sidecar verification.
// For GLOAS, kzg_commitments come from block.body.signed_execution_payload_bid.message.blob_kzg_commitments.
// [New in Gloas:EIP7732]
func (d *peerdas) getKzgCommitmentsForGloas(slot uint64, blockRoot common.Hash) (*solid.ListSSZ[*cltypes.KZGCommitment], error) {
	data, err := d.getGloasData(blockRoot)
	if err != nil {
		return nil, err
	}
	if data == nil || data.BlobKzgCommitments == nil {
		return nil, errors.New("kzg_commitments not found in block for GLOAS")
	}
	return data.BlobKzgCommitments, nil
}

// getSignedBlockHeaderForGloas retrieves SignedBlockHeader for GLOAS blob recovery.
// For GLOAS, SignedBlockHeader is not in the sidecar, so we get it from the block.
// [New in Gloas:EIP7732]
func (d *peerdas) getSignedBlockHeaderForGloas(blockRoot common.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	data, err := d.getGloasData(blockRoot)
	if err != nil {
		return nil, err
	}
	if data == nil || data.SignedBeaconBlockHeader == nil {
		return nil, errors.New("SignedBlockHeader not found in block for GLOAS")
	}
	return data.SignedBeaconBlockHeader, nil
}

func (d *peerdas) IsBlobAlreadyRecovered(blockRoot common.Hash) bool {
	count, err := d.blobStorage.KzgCommitmentsCount(context.Background(), blockRoot)
	if err != nil {
		log.Warn("failed to get kzg commitments count", "err", err, "blockRoot", blockRoot)
		return false
	}
	return count > 0
}

type blobRecoveryMetadata struct {
	slot         uint64
	blockRoot    common.Hash
	version      clparams.StateVersion
	signature    common.Bytes96
	hasSignature bool
	commitments  []common.Bytes48
}

func newBlobRecoveryMetadata(block cltypes.ColumnSyncableSignedBlock, blockRoot common.Hash) (*blobRecoveryMetadata, error) {
	commitments := block.GetBlobKzgCommitments()
	if commitments == nil {
		return nil, errors.New("missing blob commitments")
	}
	metadata := &blobRecoveryMetadata{
		slot:        block.GetSlot(),
		blockRoot:   blockRoot,
		version:     block.Version(),
		commitments: make([]common.Bytes48, commitments.Len()),
	}
	switch block := block.(type) {
	case *cltypes.SignedBeaconBlock:
		metadata.signature = block.Signature
		metadata.hasSignature = true
	case *cltypes.SignedBlindedBeaconBlock:
		metadata.signature = block.Signature
		metadata.hasSignature = true
	}
	for i := range commitments.Len() {
		commitment := commitments.Get(i)
		if commitment == nil {
			return nil, errors.New("nil blob commitment")
		}
		metadata.commitments[i] = common.Bytes48(*commitment)
	}
	return metadata, nil
}

type blobRecoveryValidation uint8

const (
	blobRecoveryUnavailable blobRecoveryValidation = iota
	blobRecoveryInvalid
	blobRecoveryComplete
)

func (d *peerdas) validateStoredBlobRecoveryMetadata(ctx context.Context, metadata *blobRecoveryMetadata, count uint32) blobRecoveryValidation {
	if metadata == nil || len(metadata.commitments) == 0 || int(count) != len(metadata.commitments) {
		if metadata != nil && len(metadata.commitments) == 0 {
			return blobRecoveryComplete
		}
		return blobRecoveryInvalid
	}
	sidecars, found, err := d.blobStorage.ReadBlobSidecars(ctx, metadata.slot, metadata.blockRoot)
	if err != nil {
		return blobRecoveryUnavailable
	}
	if !found || len(sidecars) != len(metadata.commitments) {
		return blobRecoveryInvalid
	}
	seen := make([]bool, len(metadata.commitments))
	for _, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil || sidecar.Index >= uint64(len(metadata.commitments)) || seen[sidecar.Index] {
			return blobRecoveryInvalid
		}
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil || root != metadata.blockRoot || sidecar.SignedBlockHeader.Header.Slot != metadata.slot || metadata.hasSignature && sidecar.SignedBlockHeader.Signature != metadata.signature || sidecar.KzgCommitment != metadata.commitments[sidecar.Index] {
			return blobRecoveryInvalid
		}
		seen[sidecar.Index] = true
	}
	if blob_storage.VerifyBlobSidecars(sidecars, metadata.version, nil) != nil {
		return blobRecoveryInvalid
	}
	return blobRecoveryComplete
}

func (d *peerdas) storedBlobRecoveryValidation(ctx context.Context, metadata *blobRecoveryMetadata) (blobRecoveryValidation, uint32) {
	if metadata == nil {
		return blobRecoveryInvalid, 0
	}
	count, err := d.blobStorage.KzgCommitmentsCount(ctx, metadata.blockRoot)
	if err != nil {
		log.Warn("failed to get kzg commitments count", "err", err, "blockRoot", metadata.blockRoot)
		return blobRecoveryUnavailable, 0
	}
	return d.validateStoredBlobRecoveryMetadata(ctx, metadata, count), count
}

func (d *peerdas) IsColumnOverHalf(slot uint64, blockRoot common.Hash) bool {
	existingColumns, err := d.columnStorage.GetSavedColumnIndex(context.Background(), slot, blockRoot)
	if err != nil {
		log.Warn("failed to get saved column index", "err", err, "blockRoot", blockRoot)
		return false
	}
	return len(existingColumns) >= int(d.beaconConfig.NumberOfColumns+1)/2
}

func (d *peerdas) IsArchivedMode() bool {
	return d.caplinConfig.ArchiveBlobs || d.caplinConfig.ImmediateBlobsBackfilling
}

func (d *peerdas) IsDataAvailable(slot uint64, blockRoot common.Hash) (bool, error) {
	if d.IsArchivedMode() {
		return d.IsColumnOverHalf(slot, blockRoot) || d.IsBlobAlreadyRecovered(blockRoot), nil
	}
	return d.isMyColumnDataAvailable(slot, blockRoot)
}

func (d *peerdas) isMyColumnDataAvailable(slot uint64, blockRoot common.Hash) (bool, error) {
	expectedCustodies, err := d.state.GetMyCustodyColumns()
	if err != nil {
		return false, err
	}
	existingColumns, err := d.columnStorage.GetSavedColumnIndex(context.Background(), slot, blockRoot)
	if err != nil {
		return false, err
	}
	if len(expectedCustodies) == 0 {
		// this case is not reasonable due to empty node ID
		return len(existingColumns) == int(d.beaconConfig.NumberOfColumns), nil
	}
	nowCustodies := map[cltypes.CustodyIndex]bool{}
	for _, column := range existingColumns {
		if _, ok := expectedCustodies[column]; ok {
			nowCustodies[column] = true
		}
	}
	return len(nowCustodies) == len(expectedCustodies), nil
}

func (d *peerdas) resubscribeGossip() {
	if d.IsArchivedMode() {
		// subscribe to all subnets
		for subnet := range d.beaconConfig.DataColumnSidecarSubnetCount {
			topicName := gossip.TopicNameDataColumnSidecar(subnet)
			expiry := time.Unix(0, math.MaxInt64)
			if err := d.gossipManager.SubscribeWithExpiry(topicName, expiry); err != nil {
				log.Warn("[peerdas] failed to subscribe to column sidecar subnet", "err", err, "subnet", subnet)
			} else {
				log.Debug("[peerdas] subscribed to column sidecar subnet", "subnet", subnet)
			}
		}
		return
	}

	// subscribe to the columns in our custody group
	custodyColumns, err := d.state.GetMyCustodyColumns()
	if err != nil {
		log.Warn("failed to get my custody columns", "err", err)
		return
	}
	for column := range custodyColumns {
		subnet := ComputeSubnetForDataColumnSidecar(column)
		topicName := gossip.TopicNameDataColumnSidecar(subnet)
		expiry := time.Unix(0, math.MaxInt64)
		if err := d.gossipManager.SubscribeWithExpiry(topicName, expiry); err != nil {
			log.Warn("[peerdas] failed to subscribe to column sidecar", "err", err, "column", column, "subnet", subnet)
		} else {
			log.Debug("[peerdas] subscribed to column sidecar", "column", column, "subnet", subnet)
		}
	}
}

func (d *peerdas) UpdateValidatorsCustody(cgc uint64) {
	adCgcChanged := d.state.SetCustodyGroupCount(cgc)
	if adCgcChanged {
		if !d.IsArchivedMode() {
			// subscribe more topics, advertised cgc is increased
			d.resubscribeGossip()
		}
	}
}

func (d *peerdas) PruneBelow(slot uint64) error {
	err := d.columnStorage.PruneBelow(slot)
	if errors.Is(err, blob_storage.ErrPruneNotStarted) {
		return err
	}
	d.pruneBlobRecoveriesBelow(slot)
	// A partial failure still advances the floor: pruneBelow attempts every bucket, so it
	// leaves stragglers rather than an untouched store, and the floor only understates them.
	if slot == 0 {
		d.state.SetEarliestAvailableSlot(0)
	} else if slot > d.state.GetEarliestAvailableSlot() {
		d.state.SetEarliestAvailableSlot(slot)
	}
	return err
}

func (d *peerdas) pruneBlobRecoveriesBelow(slot uint64) {
	d.initBlobRecoveryRetries()
	d.recoveryRetryMutex.Lock()
	d.recoveringMutex.Lock()
	d.initBlobRecoveryOwnershipLocked()
	if slot > d.recoveryPruneFloor {
		d.recoveryPruneFloor = slot
	}
	for d.recoverySlotQueue.Len() > 0 && d.recoverySlotQueue[0].slot < d.recoveryPruneFloor {
		ownedSlot := heap.Pop(&d.recoverySlotQueue).(*blobRecoverySlot)
		delete(d.recoverySlots, ownedSlot.slot)
		for root := range ownedSlot.roots {
			d.removeBlobRecoveryResultLocked(root)
			if d.isRecovering[root] {
				continue
			}
			d.removeDelayedBlobRecoveryLocked(root)
			delete(d.isRecovering, root)
			delete(d.recoveryRequests, root)
			delete(d.recoveryGenerations, root)
		}
	}
	d.recoveringMutex.Unlock()
	d.recoveryRetryMutex.Unlock()
	select {
	case d.recoveryRetryWake <- struct{}{}:
	default:
	}
}

type recoverBlobsRequest struct {
	slot      uint64
	blockRoot common.Hash
	metadata  *blobRecoveryMetadata
}

type delayedRecoverBlobsRequest struct {
	request   recoverBlobsRequest
	notBefore time.Time
	heapIndex int
}

type blobRecoveryRetryHeap []*delayedRecoverBlobsRequest

func (h blobRecoveryRetryHeap) Len() int { return len(h) }

func (h blobRecoveryRetryHeap) Less(i, j int) bool {
	return h[i].notBefore.Before(h[j].notBefore)
}

func (h blobRecoveryRetryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *blobRecoveryRetryHeap) Push(value any) {
	delayed := value.(*delayedRecoverBlobsRequest)
	delayed.heapIndex = len(*h)
	*h = append(*h, delayed)
}

func (h *blobRecoveryRetryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	delayed := old[last]
	old[last] = nil
	delayed.heapIndex = -1
	*h = old[:last]
	return delayed
}

type blobRecoverySlot struct {
	slot      uint64
	roots     map[common.Hash]struct{}
	heapIndex int
}

type blobRecoverySlotHeap []*blobRecoverySlot

func (h blobRecoverySlotHeap) Len() int { return len(h) }

func (h blobRecoverySlotHeap) Less(i, j int) bool { return h[i].slot < h[j].slot }

func (h blobRecoverySlotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *blobRecoverySlotHeap) Push(value any) {
	slot := value.(*blobRecoverySlot)
	slot.heapIndex = len(*h)
	*h = append(*h, slot)
}

func (h *blobRecoverySlotHeap) Pop() any {
	old := *h
	last := len(old) - 1
	slot := old[last]
	old[last] = nil
	slot.heapIndex = -1
	*h = old[:last]
	return slot
}

type blobRecoveryResult struct {
	existingColumns         []uint64
	isGloas                 bool
	kzgCommitmentsFromBlock *solid.ListSSZ[*cltypes.KZGCommitment]
	anyColumnSidecar        *cltypes.DataColumnSidecar
	blobMatrix              [][]cltypes.MatrixEntry
	blobSidecars            []*cltypes.BlobSidecar
	numberOfBlobs           uint64
	timeRecoverMatrix       time.Duration
	timeRecoverBlobs        time.Duration
	encodedBytes            int
}

func newBlobRecoveryResult(
	existingColumns []uint64,
	isGloas bool,
	kzgCommitmentsFromBlock *solid.ListSSZ[*cltypes.KZGCommitment],
	anyColumnSidecar *cltypes.DataColumnSidecar,
	blobMatrix [][]cltypes.MatrixEntry,
	blobSidecars []*cltypes.BlobSidecar,
	numberOfBlobs uint64,
	timeRecoverMatrix time.Duration,
	timeRecoverBlobs time.Duration,
) *blobRecoveryResult {
	encodedBytes := len(existingColumns) * 8
	if anyColumnSidecar != nil {
		encodedBytes += anyColumnSidecar.EncodingSizeSSZ()
	}
	if kzgCommitmentsFromBlock != nil {
		encodedBytes += kzgCommitmentsFromBlock.EncodingSizeSSZ()
	}
	for _, entries := range blobMatrix {
		for i := range entries {
			encodedBytes += entries[i].EncodingSizeSSZ()
		}
	}
	for _, sidecar := range blobSidecars {
		encodedBytes += sidecar.EncodingSizeSSZ()
	}
	return &blobRecoveryResult{
		existingColumns:         existingColumns,
		isGloas:                 isGloas,
		kzgCommitmentsFromBlock: kzgCommitmentsFromBlock,
		anyColumnSidecar:        anyColumnSidecar,
		blobMatrix:              blobMatrix,
		blobSidecars:            blobSidecars,
		numberOfBlobs:           numberOfBlobs,
		timeRecoverMatrix:       timeRecoverMatrix,
		timeRecoverBlobs:        timeRecoverBlobs,
		encodedBytes:            encodedBytes,
	}
}

func (d *peerdas) initBlobRecoveryRetries() {
	d.recoveryRetryOnce.Do(func() {
		if d.recoveryRetries == nil {
			d.recoveryRetries = make(map[common.Hash]*delayedRecoverBlobsRequest)
		}
		if d.recoveryRetryWake == nil {
			d.recoveryRetryWake = make(chan struct{}, numOfBlobRecoveryWorkers)
		}
	})
}

func (d *peerdas) initBlobRecoveryOwnershipLocked() {
	if d.isRecovering == nil {
		d.isRecovering = make(map[common.Hash]bool)
	}
	if d.recoveryRequests == nil {
		d.recoveryRequests = make(map[common.Hash]recoverBlobsRequest)
	}
	if d.recoveryGenerations == nil {
		d.recoveryGenerations = make(map[common.Hash]uint64)
	}
	if d.recoveryResults == nil {
		d.recoveryResults = make(map[common.Hash]*blobRecoveryResult)
	}
	if d.recoverySlots == nil {
		d.recoverySlots = make(map[uint64]*blobRecoverySlot)
	}
}

func blobRecoveryRetryServiceInterval() time.Duration {
	workers := max(numOfBlobRecoveryWorkers, 1)
	return blobRecoveryValidationRetryInterval / time.Duration(workers)
}

func (d *peerdas) trackBlobRecoverySlotLocked(request recoverBlobsRequest) {
	slot := d.recoverySlots[request.slot]
	if slot == nil {
		slot = &blobRecoverySlot{slot: request.slot, roots: make(map[common.Hash]struct{})}
		d.recoverySlots[request.slot] = slot
		heap.Push(&d.recoverySlotQueue, slot)
	}
	slot.roots[request.blockRoot] = struct{}{}
}

func (d *peerdas) removeBlobRecoverySlotLocked(request recoverBlobsRequest) {
	slot := d.recoverySlots[request.slot]
	if slot == nil {
		return
	}
	delete(slot.roots, request.blockRoot)
	if len(slot.roots) == 0 {
		heap.Remove(&d.recoverySlotQueue, slot.heapIndex)
		delete(d.recoverySlots, request.slot)
	}
}

func (d *peerdas) removeBlobRecoveryOwnershipLocked(blockRoot common.Hash) {
	request, exists := d.recoveryRequests[blockRoot]
	delete(d.isRecovering, blockRoot)
	delete(d.recoveryRequests, blockRoot)
	delete(d.recoveryGenerations, blockRoot)
	d.removeBlobRecoveryResultLocked(blockRoot)
	if exists {
		d.removeBlobRecoverySlotLocked(request)
	}
}

func (d *peerdas) removeDelayedBlobRecoveryLocked(blockRoot common.Hash) {
	delayed := d.recoveryRetries[blockRoot]
	if delayed == nil {
		return
	}
	heap.Remove(&d.recoveryRetryQueue, delayed.heapIndex)
	delete(d.recoveryRetries, blockRoot)
}

func (d *peerdas) blobRecoveryResult(blockRoot common.Hash) *blobRecoveryResult {
	d.recoveringMutex.Lock()
	defer d.recoveringMutex.Unlock()
	return d.recoveryResults[blockRoot]
}

func (d *peerdas) authoritativeBlobRecoveryRequest(request recoverBlobsRequest) recoverBlobsRequest {
	d.recoveringMutex.Lock()
	defer d.recoveringMutex.Unlock()
	if owned, exists := d.recoveryRequests[request.blockRoot]; exists {
		return owned
	}
	return request
}

func (d *peerdas) cacheBlobRecoveryResult(blockRoot common.Hash, result *blobRecoveryResult) bool {
	if result == nil || result.encodedBytes > maxBlobRecoveryResultBytes {
		return false
	}
	d.recoveringMutex.Lock()
	defer d.recoveringMutex.Unlock()
	d.initBlobRecoveryOwnershipLocked()
	if request, exists := d.recoveryRequests[blockRoot]; exists && request.slot < d.recoveryPruneFloor {
		return false
	}
	d.removeBlobRecoveryResultLocked(blockRoot)
	for d.recoveryResultBytes+result.encodedBytes > maxBlobRecoveryResultBytes && len(d.recoveryResultOrder) > 0 {
		evictedRoot := d.recoveryResultOrder[0]
		d.removeBlobRecoveryResultLocked(evictedRoot)
	}
	d.recoveryResultOrder = append(d.recoveryResultOrder, blockRoot)
	d.recoveryResults[blockRoot] = result
	d.recoveryResultBytes += result.encodedBytes
	return true
}

func (d *peerdas) removeBlobRecoveryResultLocked(blockRoot common.Hash) {
	result := d.recoveryResults[blockRoot]
	if result == nil {
		return
	}
	d.recoveryResultBytes -= result.encodedBytes
	delete(d.recoveryResults, blockRoot)
	for i, root := range d.recoveryResultOrder {
		if root == blockRoot {
			copy(d.recoveryResultOrder[i:], d.recoveryResultOrder[i+1:])
			d.recoveryResultOrder = d.recoveryResultOrder[:len(d.recoveryResultOrder)-1]
			break
		}
	}
}

func (d *peerdas) removeBlobRecoveryResult(blockRoot common.Hash) {
	d.recoveringMutex.Lock()
	d.removeBlobRecoveryResultLocked(blockRoot)
	d.recoveringMutex.Unlock()
}

func (d *peerdas) delayBlobRecovery(request recoverBlobsRequest) bool {
	d.initBlobRecoveryRetries()
	d.recoveryRetryMutex.Lock()
	d.recoveringMutex.Lock()
	d.initBlobRecoveryOwnershipLocked()
	if owned, exists := d.recoveryRequests[request.blockRoot]; exists {
		request = owned
	} else {
		d.recoveryRequests[request.blockRoot] = request
		d.recoveryGenerations[request.blockRoot] = 1
		d.trackBlobRecoverySlotLocked(request)
	}
	if request.slot < d.recoveryPruneFloor {
		d.removeDelayedBlobRecoveryLocked(request.blockRoot)
		d.removeBlobRecoveryOwnershipLocked(request.blockRoot)
		d.recoveringMutex.Unlock()
		d.recoveryRetryMutex.Unlock()
		return false
	}
	d.isRecovering[request.blockRoot] = false
	d.recoveringMutex.Unlock()

	notBefore := time.Now().Add(blobRecoveryValidationRetryInterval)
	if existing := d.recoveryRetries[request.blockRoot]; existing != nil {
		existing.request = request
		existing.notBefore = notBefore
		heap.Fix(&d.recoveryRetryQueue, existing.heapIndex)
	} else {
		delayed := &delayedRecoverBlobsRequest{request: request, notBefore: notBefore, heapIndex: -1}
		d.recoveryRetries[request.blockRoot] = delayed
		heap.Push(&d.recoveryRetryQueue, delayed)
	}
	d.recoveryRetryMutex.Unlock()
	select {
	case d.recoveryRetryWake <- struct{}{}:
	default:
	}
	return true
}

func (d *peerdas) nextBlobRecoveryRequest(ctx context.Context) (recoverBlobsRequest, bool) {
	d.initBlobRecoveryRetries()
	for {
		now := time.Now()
		var next time.Time
		d.recoveryRetryMutex.Lock()
		if d.recoveryRetryQueue.Len() > 0 {
			next = d.recoveryRetryQueue[0].notBefore
			if d.recoveryRetryAfter.After(next) {
				next = d.recoveryRetryAfter
			}
		}
		if !next.IsZero() && !next.After(now) {
			if !d.recoveryPreferRetry {
				select {
				case request := <-d.recoverBlobsQueue:
					d.recoveryPreferRetry = true
					d.recoveryRetryMutex.Unlock()
					return request, true
				default:
				}
			}
			delayed := heap.Pop(&d.recoveryRetryQueue).(*delayedRecoverBlobsRequest)
			delete(d.recoveryRetries, delayed.request.blockRoot)
			d.recoveryRetryAfter = now.Add(blobRecoveryRetryServiceInterval())
			d.recoveryPreferRetry = false
			d.recoveryRetryMutex.Unlock()
			return delayed.request, true
		}
		d.recoveryRetryMutex.Unlock()

		if next.IsZero() {
			select {
			case <-ctx.Done():
				return recoverBlobsRequest{}, false
			case request := <-d.recoverBlobsQueue:
				return request, true
			case <-d.recoveryRetryWake:
			}
			continue
		}

		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return recoverBlobsRequest{}, false
		case request := <-d.recoverBlobsQueue:
			timer.Stop()
			return request, true
		case <-d.recoveryRetryWake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (d *peerdas) claimBlobRecovery(request recoverBlobsRequest) (recoverBlobsRequest, uint64, bool) {
	d.recoveringMutex.Lock()
	defer d.recoveringMutex.Unlock()
	d.initBlobRecoveryOwnershipLocked()
	if request.slot < d.recoveryPruneFloor {
		d.removeBlobRecoveryOwnershipLocked(request.blockRoot)
		return recoverBlobsRequest{}, 0, false
	}
	active, exists := d.isRecovering[request.blockRoot]
	if active {
		return recoverBlobsRequest{}, 0, false
	}
	if !exists {
		d.isRecovering[request.blockRoot] = false
		d.recoveryRequests[request.blockRoot] = request
		d.recoveryGenerations[request.blockRoot] = 1
		d.trackBlobRecoverySlotLocked(request)
	}
	owned, exists := d.recoveryRequests[request.blockRoot]
	if !exists {
		owned = request
		d.recoveryRequests[request.blockRoot] = request
		d.recoveryGenerations[request.blockRoot] = 1
		d.trackBlobRecoverySlotLocked(request)
	}
	d.isRecovering[request.blockRoot] = true
	return owned, d.recoveryGenerations[request.blockRoot], true
}

func (d *peerdas) releaseBlobRecovery(blockRoot common.Hash, generation uint64) {
	d.recoveringMutex.Lock()
	request, exists := d.recoveryRequests[blockRoot]
	if !exists {
		d.recoveringMutex.Unlock()
		return
	}
	if request.slot < d.recoveryPruneFloor {
		d.removeBlobRecoveryOwnershipLocked(blockRoot)
		d.recoveringMutex.Unlock()
		return
	}
	if current := d.recoveryGenerations[blockRoot]; current != generation {
		d.isRecovering[blockRoot] = false
		d.recoveringMutex.Unlock()
		d.delayBlobRecovery(request)
		return
	}
	d.removeBlobRecoveryOwnershipLocked(blockRoot)
	d.recoveringMutex.Unlock()
}

func (d *peerdas) clearDelayedBlobRecoveries() {
	d.initBlobRecoveryRetries()
	d.recoveryRetryMutex.Lock()
	d.recoveringMutex.Lock()
	d.recoveryRetries = make(map[common.Hash]*delayedRecoverBlobsRequest)
	d.recoveryRetryQueue = nil
	d.recoveryRetryAfter = time.Time{}
	d.recoveryPreferRetry = false
	for root, active := range d.isRecovering {
		if !active {
			d.removeBlobRecoveryOwnershipLocked(root)
		}
	}
	d.recoveryResults = make(map[common.Hash]*blobRecoveryResult)
	d.recoveryResultOrder = nil
	d.recoveryResultBytes = 0
	d.recoveringMutex.Unlock()
	d.recoveryRetryMutex.Unlock()
}

func (d *peerdas) enqueueBlobRecovery(request recoverBlobsRequest) error {
	d.recoveringMutex.Lock()
	d.initBlobRecoveryOwnershipLocked()
	if request.slot < d.recoveryPruneFloor {
		d.recoveringMutex.Unlock()
		return nil
	}
	if _, ok := d.isRecovering[request.blockRoot]; ok {
		owned := d.recoveryRequests[request.blockRoot]
		if owned.metadata == nil && request.metadata != nil {
			d.recoveryRequests[request.blockRoot] = request
			d.recoveryGenerations[request.blockRoot]++
		}
		d.recoveringMutex.Unlock()
		return nil
	}
	d.isRecovering[request.blockRoot] = false
	d.recoveryRequests[request.blockRoot] = request
	d.recoveryGenerations[request.blockRoot] = 1
	d.trackBlobRecoverySlotLocked(request)
	select {
	case d.recoverBlobsQueue <- request:
		d.recoveringMutex.Unlock()
		return nil
	default:
		d.recoveringMutex.Unlock()
		d.delayBlobRecovery(request)
		return nil
	}
}

func (d *peerdas) blobsRecoverWorker(ctx context.Context) {
	defer d.clearDelayedBlobRecoveries()
	recover := func(toRecover recoverBlobsRequest) (retryScheduled bool) {
		if ctx.Err() != nil {
			return
		}
		begin := time.Now()
		log.Debug("[blobsRecover] recovering blobs", "slot", toRecover.slot, "blockRoot", toRecover.blockRoot)
		slot, blockRoot := toRecover.slot, toRecover.blockRoot
		result := d.blobRecoveryResult(blockRoot)
		if result == nil {
			result = func() (result *blobRecoveryResult) {
				existingColumns, err := d.columnStorage.GetSavedColumnIndex(ctx, slot, blockRoot)
				if err != nil {
					log.Warn("[blobsRecover] failed to get saved column index", "err", err)
					if ctx.Err() == nil {
						retryScheduled = d.delayBlobRecovery(toRecover)
					}
					return
				}
				if ctx.Err() != nil {
					return
				}
				if len(existingColumns) < int(d.beaconConfig.NumberOfColumns+1)/2 {
					log.Debug("[blobsRecover] not enough columns to recover", "slot", slot, "blockRoot", blockRoot, "existingColumns", len(existingColumns))
					return
				}

				// [Modified in Gloas:EIP7732] For GLOAS, kzg_commitments and SignedBlockHeader come from block
				epoch := slot / d.beaconConfig.SlotsPerEpoch
				isGloas := d.beaconConfig.GetCurrentStateVersion(epoch) >= clparams.GloasVersion
				var kzgCommitmentsFromBlock *solid.ListSSZ[*cltypes.KZGCommitment]
				var signedBlockHeaderFromBlock *cltypes.SignedBeaconBlockHeader
				if isGloas {
					kzgCommitmentsFromBlock, err = d.getKzgCommitmentsForGloas(slot, blockRoot)
					if err != nil {
						log.Warn("[blobsRecover] failed to get kzg commitments for GLOAS", "err", err, "slot", slot, "blockRoot", blockRoot)
						return
					}
					signedBlockHeaderFromBlock, err = d.getSignedBlockHeaderForGloas(blockRoot)
					if err != nil {
						log.Warn("[blobsRecover] failed to get signed block header for GLOAS", "err", err, "slot", slot, "blockRoot", blockRoot)
						return
					}
				}

				// Recover the matrix from the column sidecars
				matrixEntries := []cltypes.MatrixEntry{}
				var anyColumnSidecar *cltypes.DataColumnSidecar
				for _, columnIndex := range existingColumns {
					if ctx.Err() != nil {
						return
					}
					sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, int64(columnIndex))
					if err != nil {
						log.Debug("[blobsRecover] failed to read column sidecar", "err", err)
						if ctx.Err() != nil {
							return
						}
						if errors.Is(err, os.ErrNotExist) {
							if removeErr := d.columnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, int64(columnIndex)); removeErr != nil {
								log.Debug("[blobsRecover] failed to remove column sidecar", "err", removeErr)
							}
						} else {
							retryScheduled = d.delayBlobRecovery(toRecover)
						}
						return
					}
					if sidecar.Column.Len() > int(d.beaconConfig.MaxBlobCommittmentsPerBlock) {
						log.Warn("[blobsRecover] invalid column sidecar", "slot", slot, "blockRoot", blockRoot, "columnIndex", columnIndex, "columnLen", sidecar.Column.Len())
						return
					}
					for i := 0; i < sidecar.Column.Len(); i++ {
						matrixEntries = append(matrixEntries, cltypes.MatrixEntry{
							Cell:        *sidecar.Column.Get(i),
							KzgProof:    *sidecar.KzgProofs.Get(i),
							RowIndex:    uint64(i),
							ColumnIndex: columnIndex,
						})
					}
					if anyColumnSidecar == nil {
						anyColumnSidecar = sidecar
					}
				}
				// recover matrix
				beginRecoverMatrix := time.Now()
				numberOfBlobs := uint64(anyColumnSidecar.Column.Len())
				if ctx.Err() != nil {
					return
				}
				blobMatrix, err := peerdasutils.RecoverMatrix(matrixEntries, numberOfBlobs)
				if err != nil {
					log.Warn("[blobsRecover] failed to recover matrix", "err", err, "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)
					return
				}
				timeRecoverMatrix := time.Since(beginRecoverMatrix)
				if ctx.Err() != nil {
					return
				}
				log.Trace("[blobsRecover] recovered matrix", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)

				// Recover blobs from the matrix
				beginRecoverBlobs := time.Now()
				blobSidecars := make([]*cltypes.BlobSidecar, 0, len(blobMatrix))
				blobCommitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](int(d.beaconConfig.MaxBlobCommittmentsPerBlock), length.Bytes48)
				for blobIndex, blobEntries := range blobMatrix {
					var (
						blob           cltypes.Blob
						kzgCommitment  common.Bytes48
						kzgProof       common.Bytes48
						inclusionProof solid.HashVectorSSZ = solid.NewHashVector(cltypes.CommitmentBranchSize)
					)
					// blob
					if len(blobEntries) != int(d.beaconConfig.NumberOfColumns) {
						log.Warn("[blobsRecover] invalid blob entries", "blobIndex", blobIndex, "slot", slot, "blockRoot", blockRoot, "blobEntries", len(blobEntries))
						return
					}
					for i := range len(blobEntries) / 2 {
						if copied := copy(blob[i*cltypes.BytesPerCell:], blobEntries[i].Cell[:]); copied != cltypes.BytesPerCell {
							log.Warn("[blobsRecover] failed to copy cell", "blobIndex", blobIndex, "slot", slot, "blockRoot", blockRoot)
							return
						}
					}
					// kzg commitment
					// [Modified in Gloas:EIP7732] Use kzg_commitments from block for GLOAS
					if isGloas {
						copy(kzgCommitment[:], kzgCommitmentsFromBlock.Get(blobIndex)[:])
					} else {
						copy(kzgCommitment[:], anyColumnSidecar.KzgCommitments.Get(blobIndex)[:])
					}
					// kzg proof
					ckzgBlob := goethkzg.Blob(blob)
					proof, err := kzg.Ctx().ComputeBlobKZGProof(&ckzgBlob, goethkzg.KZGCommitment(kzgCommitment), 0 /* numGoRoutines */)
					if err != nil {
						log.Warn("[blobsRecover] failed to compute blob kzg proof", "blobIndex", blobIndex, "slot", slot, "blockRoot", blockRoot)
						return
					}
					copy(kzgProof[:], proof[:])
					// [Modified in Gloas:EIP7732] Use SignedBlockHeader from block for GLOAS
					var signedBlockHeader *cltypes.SignedBeaconBlockHeader
					if isGloas {
						signedBlockHeader = signedBlockHeaderFromBlock
					} else {
						signedBlockHeader = anyColumnSidecar.SignedBlockHeader
					}
					blobSidecar := cltypes.NewBlobSidecar(
						uint64(blobIndex),
						&blob,
						kzgCommitment,
						kzgProof,
						signedBlockHeader,
						inclusionProof,
					)
					blobSidecars = append(blobSidecars, blobSidecar)
					commitment := cltypes.KZGCommitment(kzgCommitment)
					blobCommitments.Append(&commitment)
				}
				timeRecoverBlobs := time.Since(beginRecoverBlobs)
				// inclusion proof
				// [Modified in Gloas:EIP7732] GLOAS sidecars don't have KzgCommitmentsInclusionProof
				if !isGloas {
					for i := range len(blobSidecars) {
						branchProof := blobCommitments.ElementProof(i)
						p := blobSidecars[i].CommitmentInclusionProof
						for index := range branchProof {
							p.Set(index, branchProof[index])
						}
						for index := range anyColumnSidecar.KzgCommitmentsInclusionProof.Length() {
							p.Set(index+len(branchProof), anyColumnSidecar.KzgCommitmentsInclusionProof.Get(index))
						}
					}
				}
				return newBlobRecoveryResult(existingColumns, isGloas, kzgCommitmentsFromBlock, anyColumnSidecar, blobMatrix, blobSidecars, numberOfBlobs, timeRecoverMatrix, timeRecoverBlobs)
			}()
			if result == nil {
				return
			}
		}
		existingColumns := result.existingColumns
		isGloas := result.isGloas
		kzgCommitmentsFromBlock := result.kzgCommitmentsFromBlock
		anyColumnSidecar := result.anyColumnSidecar
		blobMatrix := result.blobMatrix
		blobSidecars := result.blobSidecars
		numberOfBlobs := result.numberOfBlobs
		timeRecoverMatrix := result.timeRecoverMatrix
		timeRecoverBlobs := result.timeRecoverBlobs
		// Save blobs
		toRecover = d.authoritativeBlobRecoveryRequest(toRecover)
		if toRecover.metadata != nil {
			validation, _ := d.storedBlobRecoveryValidation(ctx, toRecover.metadata)
			if validation == blobRecoveryUnavailable && ctx.Err() == nil {
				d.cacheBlobRecoveryResult(blockRoot, result)
				return d.delayBlobRecovery(toRecover)
			}
			if validation != blobRecoveryInvalid {
				return
			}
		} else {
			count, err := d.blobStorage.KzgCommitmentsCount(ctx, blockRoot)
			if err != nil && ctx.Err() == nil {
				d.cacheBlobRecoveryResult(blockRoot, result)
				return d.delayBlobRecovery(toRecover)
			}
			if err != nil || count > 0 {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err := d.blobStorage.WriteBlobSidecars(ctx, blockRoot, blobSidecars); err != nil {
			log.Warn("[blobsRecover] failed to write blob sidecars", "err", err, "slot", slot, "blockRoot", blockRoot)
			if ctx.Err() == nil {
				d.cacheBlobRecoveryResult(blockRoot, result)
				return d.delayBlobRecovery(toRecover)
			}
			return
		}
		d.removeBlobRecoveryResult(blockRoot)
		log.Trace("[blobsRecover] saved blobs", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)

		// remove column sidecars that are not in our custody group
		custodyColumns, err := d.state.GetMyCustodyColumns()
		if err != nil {
			log.Warn("[blobsRecover] failed to get my custody columns", "err", err, "slot", slot, "blockRoot", blockRoot)
			return
		}
		beginRemoveColumns := time.Now()
		toRemove := []int64{}
		for _, column := range existingColumns {
			if _, ok := custodyColumns[column]; !ok {
				toRemove = append(toRemove, int64(column))
			}
		}
		if err := d.columnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, toRemove...); err != nil {
			log.Warn("[blobsRecover] failed to remove column sidecars", "err", err, "slot", slot, "blockRoot", blockRoot, "columns", toRemove)
		}
		timeRemoveColumns := time.Since(beginRemoveColumns)
		// add custody data column if it doesn't exist
		beginAddColumns := time.Now()
		for columnIndex := range custodyColumns {
			exist, err := d.columnStorage.ColumnSidecarExists(ctx, slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Warn("[blobsRecover] failed to check if column sidecar exists", "err", err, "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
				continue
			}
			if !exist {
				blobSize := anyColumnSidecar.Column.Len()
				version := d.beaconConfig.GetCurrentStateVersion(slot / d.beaconConfig.SlotsPerEpoch)
				sidecar := cltypes.NewDataColumnSidecarWithVersion(version)
				sidecar.Index = columnIndex
				// [Modified in Gloas:EIP7732] GLOAS sidecars don't have KzgCommitmentsInclusionProof and KzgCommitments
				if isGloas {
					sidecar.Slot = slot
					sidecar.BeaconBlockRoot = blockRoot
				} else {
					sidecar.SignedBlockHeader = anyColumnSidecar.SignedBlockHeader
					sidecar.KzgCommitmentsInclusionProof = anyColumnSidecar.KzgCommitmentsInclusionProof
					sidecar.KzgCommitments = anyColumnSidecar.KzgCommitments
				}
				for i := range blobSize {
					// cell
					sidecar.Column.Append(&blobMatrix[i][columnIndex].Cell)
					// kzg proof
					sidecar.KzgProofs.Append(&blobMatrix[i][columnIndex].KzgProof)
				}
				// verify the sidecar
				// [Modified in Gloas:EIP7732] Version-aware verification
				if isGloas {
					if !VerifyDataColumnSidecarWithCommitments(sidecar, kzgCommitmentsFromBlock) {
						log.Warn("[blobsRecover] failed to verify column sidecar (GLOAS)", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
						continue
					}
					if !VerifyDataColumnSidecarKZGProofsWithCommitments(sidecar, kzgCommitmentsFromBlock) {
						log.Warn("[blobsRecover] failed to verify column sidecar kzg proofs (GLOAS)", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
						continue
					}
				} else {
					if !VerifyDataColumnSidecar(sidecar) {
						log.Warn("[blobsRecover] failed to verify column sidecar", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
						continue
					}
					if !VerifyDataColumnSidecarInclusionProof(sidecar) {
						log.Warn("[blobsRecover] failed to verify column sidecar inclusion proof", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
						continue
					}
					if !VerifyDataColumnSidecarKZGProofs(sidecar) {
						log.Warn("[blobsRecover] failed to verify column sidecar kzg proofs", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
						continue
					}
				}
				// save the sidecar to the column storage
				if err := d.columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(columnIndex), sidecar); err != nil {
					log.Warn("[blobsRecover] failed to write column sidecar", "err", err, "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
					continue
				}
				log.Trace("[blobsRecover] added a custody data column", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
			}
		}
		timeAddColumns := time.Since(beginAddColumns)
		log.Debug("[blobsRecover] recovering done", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs, "elapsedTime", time.Since(begin),
			"timeRecoverMatrix", timeRecoverMatrix, "timeRecoverBlobs", timeRecoverBlobs, "timeRemoveColumns", timeRemoveColumns, "timeAddColumns", timeAddColumns)
		return false
	}

	// main loop
	for {
		toRecover, ok := d.nextBlobRecoveryRequest(ctx)
		if !ok {
			return
		}
		var generation uint64
		toRecover, generation, ok = d.claimBlobRecovery(toRecover)
		if !ok {
			continue
		}

		var retryScheduled bool
		if toRecover.metadata != nil {
			validation, _ := d.storedBlobRecoveryValidation(ctx, toRecover.metadata)
			switch validation {
			case blobRecoveryUnavailable:
				if ctx.Err() == nil {
					retryScheduled = d.delayBlobRecovery(toRecover)
				}
			case blobRecoveryInvalid:
				retryScheduled = recover(toRecover)
			}
		} else {
			count, err := d.blobStorage.KzgCommitmentsCount(ctx, toRecover.blockRoot)
			if err != nil {
				if ctx.Err() == nil {
					retryScheduled = d.delayBlobRecovery(toRecover)
				}
			} else if count == 0 && ctx.Err() == nil {
				retryScheduled = recover(toRecover)
			}
		}
		if !retryScheduled {
			d.releaseBlobRecovery(toRecover.blockRoot, generation)
		}
	}
}

func (d *peerdas) TryScheduleRecover(slot uint64, blockRoot common.Hash) error {
	return d.tryScheduleRecover(slot, blockRoot, nil)
}

func (d *peerdas) tryScheduleRecover(slot uint64, blockRoot common.Hash, metadata *blobRecoveryMetadata) error {
	if !d.IsArchivedMode() && !d.StateReader().IsSupernode() {
		return nil
	}

	if !d.IsColumnOverHalf(slot, blockRoot) || metadata == nil && d.IsBlobAlreadyRecovered(blockRoot) {
		// no need to recover if column data is not over 50% or the blobs are already recovered
		return nil
	}

	request := recoverBlobsRequest{
		slot:      slot,
		blockRoot: blockRoot,
		metadata:  metadata,
	}
	return d.enqueueBlobRecovery(request)
}

var allColumns = func() map[cltypes.CustodyIndex]bool {
	columns := map[cltypes.CustodyIndex]bool{}
	for i := range 128 {
		columns[cltypes.CustodyIndex(i)] = true
	}
	return columns
}()

// DownloadMissingColumns downloads the missing columns for the given blocks but not recover the blobs
func (d *peerdas) DownloadOnlyCustodyColumns(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error {
	custodyColumns, err := d.state.GetMyCustodyColumns()
	if err != nil {
		return err
	}

	batchBlcokSize := 4
	wg := sync.WaitGroup{}
	for i := 0; i < len(blocks); i += batchBlcokSize {
		blocks := blocks[i:min(i+batchBlcokSize, len(blocks))]
		wg.Go(func() {
			req, err := initializeDownloadRequest(blocks, d.beaconConfig, d.columnStorage, custodyColumns)
			if err != nil {
				log.Warn("failed to initialize download request", "err", err)
				return
			}
			d.runDownload(ctx, req, false)
		})
	}
	wg.Wait()
	return nil
}

func (d *peerdas) DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error {
	// filter out blocks that don't need to be processed
	recoveryDetails := []*blobRecoveryMetadata{}
	validatedBlobCounts := make(map[common.Hash]uint32)
	for _, block := range blocks {
		kzgCommitments := block.GetBlobKzgCommitments()
		if block.Version() < clparams.FuluVersion ||
			kzgCommitments == nil ||
			kzgCommitments.Len() == 0 {
			continue
		}
		root, err := block.BlockHashSSZ()
		if err != nil {
			log.Warn("failed to get block root", "err", err)
			continue
		}

		metadata, err := newBlobRecoveryMetadata(block, root)
		if err != nil {
			log.Warn("failed to build blob recovery metadata", "err", err, "blockRoot", root)
			continue
		}
		validation, count := d.storedBlobRecoveryValidation(ctx, metadata)
		if validation != blobRecoveryUnavailable {
			validatedBlobCounts[root] = count
		}
		complete := validation == blobRecoveryComplete
		if d.IsColumnOverHalf(block.GetSlot(), root) || complete {
			if !complete {
				if err := d.tryScheduleRecover(block.GetSlot(), root, metadata); err != nil {
					log.Debug("failed to schedule recover", "err", err)
				}
			}
			continue
		}
		recoveryDetails = append(recoveryDetails, metadata)
	}

	if len(recoveryDetails) == 0 {
		return nil
	}

	begin := time.Now()
	defer func() {
		slots := make([]uint64, 0, len(blocks))
		for _, block := range blocks {
			slots = append(slots, block.GetSlot())
		}
		log.Debug("DownloadColumnsAndRecoverBlobs", "elapsed", time.Since(begin), "slots", slots)
	}()

	// initialize the download request
	batchBlcokSize := 4
	wg := sync.WaitGroup{}
	for i := 0; i < len(recoveryDetails); i += batchBlcokSize {
		details := recoveryDetails[i:min(i+batchBlcokSize, len(recoveryDetails))]
		wg.Go(func() {
			req, err := initializeDownloadRequestFromRecoveryDetails(details, validatedBlobCounts, d.beaconConfig, d.columnStorage, allColumns)
			if err != nil {
				log.Warn("failed to initialize download request", "err", err)
				return
			}
			d.runDownload(ctx, req, true)
		})
	}
	wg.Wait()
	return nil
}

func (d *peerdas) runDownload(ctx context.Context, req *downloadRequest, needToRecoverBlobs bool) {
	type resolvedColumn struct {
		slot      uint64
		blockRoot common.Hash
	}
	type resultData struct {
		sidecars  []*cltypes.DataColumnSidecar
		pid       string
		reqLength int
		requested map[requestedDataColumn]struct{}
		err       error
	}
	if req.remainingEntriesCount() == 0 {
		return
	}

	stopChan := make(chan struct{})
	defer close(stopChan)
	resultChan := make(chan resultData, 64)
	go func(req *downloadRequest) {
		// send the request in a loop with a ticker to avoid overwhelming the peer
		// keep trying until the request is done
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		wg := sync.WaitGroup{}
	loop:
		for {
			select {
			case <-stopChan:
				break loop
			case <-ticker.C:
				wg.Go(func() {
					cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					ids, requested := req.requestData()
					if ids.Len() == 0 {
						return
					}
					s, pid, filtered, err := d.rpc.SendColumnSidecarsByRootIdentifierReqWithSnapshot(cctx, ids)
					requested = requestedDataColumnsForPayload(requested, filtered)
					reqLength := len(requested)
					select {
					case resultChan <- resultData{
						sidecars:  s,
						pid:       pid,
						reqLength: reqLength,
						requested: requested,
						err:       err,
					}:
					default:
						// just drop it if the channel is full
					}
				})
			}
		}
		wg.Wait()
		close(resultChan)
	}(req)

	// check if the column data is over half at the same time because we might also receive the column sidecars from other peers
	halfCheckTicker := time.NewTicker(500 * time.Millisecond)
	defer halfCheckTicker.Stop()
mainloop:
	for {
		select {
		case <-ctx.Done():
			break mainloop
		case <-halfCheckTicker.C:
			for _, entry := range req.remainingEntries() {
				if needToRecoverBlobs {
					metadata := req.recovery(entry.blockRoot)
					if d.IsColumnOverHalf(entry.slot, entry.blockRoot) {
						req.removeBlock(entry.slot, entry.blockRoot)
						if err := d.tryScheduleRecover(entry.slot, entry.blockRoot, metadata); err != nil {
							log.Debug("failed to schedule recover", "err", err)
						}
					} else if metadata != nil {
						count, err := d.blobStorage.KzgCommitmentsCount(ctx, entry.blockRoot)
						if err == nil && req.blobCountNeedsValidation(entry.blockRoot, count) {
							validation := d.validateStoredBlobRecoveryMetadata(ctx, metadata, count)
							if validation != blobRecoveryUnavailable {
								req.updateValidatedBlobCount(entry.blockRoot, count)
							}
							if validation == blobRecoveryComplete {
								req.removeBlock(entry.slot, entry.blockRoot)
							}
						}
					}
				} else {
					available, err := d.isMyColumnDataAvailable(entry.slot, entry.blockRoot)
					if err != nil {
						log.Debug("failed to check if column data is available", "err", err)
						continue
					}
					if available {
						req.removeBlock(entry.slot, entry.blockRoot)
					}
				}
			}
			if req.remainingEntriesCount() == 0 {
				break mainloop
			}
		case result := <-resultChan:
			if result.err != nil {
				if isExpectedColumnDownloadMiss(result.err) {
					log.Trace("column sidecars unavailable from peer", "pid", result.pid, "err", result.err)
					continue
				}
				log.Debug("failed to download columns from peer", "pid", result.pid, "err", result.err)
				//d.rpc.BanPeer(result.pid)
				continue
			}
			if len(result.sidecars) == 0 {
				continue
			}
			log.Debug("received column sidecars", "pid", result.pid, "reqLength", result.reqLength, "count", len(result.sidecars))
			if len(result.sidecars) > result.reqLength {
				log.Debug("rejecting over-cardinality column response", "pid", result.pid, "reqLength", result.reqLength, "count", len(result.sidecars))
				d.rpc.BanPeer(result.pid)
				continue
			}
			resolved := make([]resolvedColumn, len(result.sidecars))
			seen := make(map[requestedDataColumn]struct{}, len(result.sidecars))
			validResponse := true
			for i, sidecar := range result.sidecars {
				if sidecar == nil {
					validResponse = false
					break
				}
				slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
				if !ok {
					validResponse = false
					break
				}
				key := requestedDataColumn{slot: slot, blockRoot: blockRoot, index: sidecar.Index}
				if _, ok := result.requested[key]; !ok {
					validResponse = false
					break
				}
				metadata := req.recovery(blockRoot)
				if sidecar.Version() < clparams.GloasVersion && metadata != nil && metadata.hasSignature && sidecar.SignedBlockHeader.Signature != metadata.signature {
					validResponse = false
					break
				}
				if _, duplicate := seen[key]; duplicate {
					validResponse = false
					break
				}
				seen[key] = struct{}{}
				resolved[i] = resolvedColumn{slot: slot, blockRoot: blockRoot}
			}
			if !validResponse {
				log.Debug("rejecting malformed, schema-inconsistent, or unrequested column response", "pid", result.pid)
				d.rpc.BanPeer(result.pid)
				continue
			}
			var wg sync.WaitGroup
			for i, sidecar := range result.sidecars {
				wg.Go(func() {
					slot, blockRoot := resolved[i].slot, resolved[i].blockRoot
					isGloasSidecar := sidecar.Version() >= clparams.GloasVersion
					defer func() {
						// check if need to schedule recover whenever we download a column sidecar
						if needToRecoverBlobs && d.IsColumnOverHalf(slot, blockRoot) {
							req.removeBlock(slot, blockRoot)
							if err := d.tryScheduleRecover(slot, blockRoot, req.recovery(blockRoot)); err != nil {
								log.Debug("failed to schedule recover", "err", err)
							}
						}
					}()

					columnIndex := sidecar.Index
					columnData := sidecar
					exist, err := d.columnStorage.ColumnSidecarExists(ctx, slot, blockRoot, int64(columnIndex))
					if err != nil {
						log.Debug("failed to check if column sidecar exists", "err", err)
						d.rpc.BanPeer(result.pid)
						return
					}
					if exist {
						req.removeColumn(slot, blockRoot, columnIndex)
						return
					}
					blobParameters := d.beaconConfig.GetBlobParameters(slot / d.beaconConfig.SlotsPerEpoch)
					if sidecar.Column.Len() > int(blobParameters.MaxBlobsPerBlock) {
						log.Warn("invalid column sidecar length", "blockRoot", blockRoot, "columnIndex", sidecar.Index, "columnLen", sidecar.Column.Len())
						d.rpc.BanPeer(result.pid)
						return
					}

					// [Modified in Gloas:EIP7732] Version-aware verification
					if isGloasSidecar {
						// GLOAS: kzg_commitments come from block
						kzgCommitments, err := d.getKzgCommitmentsForGloas(slot, blockRoot)
						if err != nil {
							log.Debug("failed to get kzg commitments for GLOAS", "err", err, "blockRoot", blockRoot)
							return
						}
						if !VerifyDataColumnSidecarWithCommitments(sidecar, kzgCommitments) {
							log.Debug("failed to verify column sidecar (GLOAS)", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
							d.rpc.BanPeer(result.pid)
							return
						}
						if !VerifyDataColumnSidecarKZGProofsWithCommitments(sidecar, kzgCommitments) {
							log.Debug("failed to verify column sidecar kzg proofs (GLOAS)", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
							d.rpc.BanPeer(result.pid)
							return
						}
					} else {
						// Fulu: kzg_commitments are in the sidecar
						if !VerifyDataColumnSidecar(sidecar) {
							log.Debug("failed to verify column sidecar", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
							d.rpc.BanPeer(result.pid)
							return
						}
						if !VerifyDataColumnSidecarInclusionProof(sidecar) {
							log.Debug("failed to verify column sidecar inclusion proof", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
							d.rpc.BanPeer(result.pid)
							return
						}
						if !VerifyDataColumnSidecarKZGProofs(sidecar) {
							log.Debug("failed to verify column sidecar kzg proofs", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
							d.rpc.BanPeer(result.pid)
							return
						}
					}
					// save the sidecar to the column storage
					if err := d.columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(columnIndex), columnData); err != nil {
						log.Debug("failed to write column sidecar", "err", err)
						return
					}
					// done. remove the column from the download table
					req.removeColumn(slot, blockRoot, columnIndex)
				})
			}
			wg.Wait()
			// check if there are any remaining requests and send again if there are
			if req.remainingEntriesCount() == 0 {
				break mainloop
			}
		}
	}
}

// resolveColumnSidecarSlotAndRoot rejects malformed sidecars and response fork versions that are not active at the claimed slot.
func (d *peerdas) resolveColumnSidecarSlotAndRoot(sidecar *cltypes.DataColumnSidecar) (slot uint64, blockRoot common.Hash, ok bool) {
	if sidecar.Version() >= clparams.GloasVersion {
		if d.beaconConfig.GetCurrentStateVersion(sidecar.Slot/d.beaconConfig.SlotsPerEpoch) != sidecar.Version() {
			return 0, common.Hash{}, false
		}
		return sidecar.Slot, sidecar.BeaconBlockRoot, true
	}
	header := sidecar.SignedBlockHeader
	if header == nil || header.Header == nil {
		return 0, common.Hash{}, false
	}
	if d.beaconConfig.GetCurrentStateVersion(header.Header.Slot/d.beaconConfig.SlotsPerEpoch) != sidecar.Version() {
		return 0, common.Hash{}, false
	}
	root, err := header.Header.HashSSZ()
	if err != nil {
		return 0, common.Hash{}, false
	}
	return header.Header.Slot, root, true
}

func isExpectedColumnDownloadMiss(err error) bool {
	if err == nil {
		return false
	}
	var peerErr *httpreqresp.PeerResponseError
	if errors.As(err, &peerErr) {
		return peerErr.Code == httpreqresp.ResponseCodeResourceUnavailable
	}
	return false
}

type downloadTableEntry struct {
	blockRoot common.Hash
	slot      uint64
}

type requestedDataColumn struct {
	slot      uint64
	blockRoot common.Hash
	index     uint64
}

func requestedDataColumnsForPayload(requested map[requestedDataColumn]struct{}, payload *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]) map[requestedDataColumn]struct{} {
	type identity struct {
		blockRoot common.Hash
		index     uint64
	}
	selected := make(map[identity]struct{})
	if payload != nil {
		payload.Range(func(_ int, id *cltypes.DataColumnsByRootIdentifier, _ int) bool {
			id.Columns.Range(func(_ int, column uint64, _ int) bool {
				selected[identity{blockRoot: id.BlockRoot, index: column}] = struct{}{}
				return true
			})
			return true
		})
	}
	filtered := make(map[requestedDataColumn]struct{}, len(selected))
	for column := range requested {
		if _, ok := selected[identity{blockRoot: column.blockRoot, index: column.index}]; ok {
			filtered[column] = struct{}{}
		}
	}
	return filtered
}

// downloadRequest is used to track the download progress of the column sidecars
type downloadRequest struct {
	beaconConfig       *clparams.BeaconChainConfig
	tableMutex         sync.RWMutex
	downloadTable      map[downloadTableEntry]map[uint64]bool
	recoveryDetails    map[common.Hash]*blobRecoveryMetadata
	validatedBlobCount map[common.Hash]uint32
}

// [Modified in Gloas:EIP7732] Changed from []*SignedBlindedBeaconBlock to []ColumnSyncableSignedBlock
func initializeDownloadRequest(
	blocks []cltypes.ColumnSyncableSignedBlock,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	expectedColumns map[cltypes.CustodyIndex]bool,
) (*downloadRequest, error) {
	details := make([]*blobRecoveryMetadata, 0, len(blocks))
	for _, block := range blocks {
		if block.Version() < clparams.FuluVersion {
			continue
		}
		kzgCommitments := block.GetBlobKzgCommitments()
		if kzgCommitments == nil || kzgCommitments.Len() == 0 {
			continue
		}

		blockRoot, err := block.BlockHashSSZ()
		if err != nil {
			return nil, err
		}
		metadata, err := newBlobRecoveryMetadata(block, blockRoot)
		if err != nil {
			return nil, err
		}
		details = append(details, metadata)
	}
	return initializeDownloadRequestFromRecoveryDetails(details, nil, beaconConfig, columnStorage, expectedColumns)
}

func initializeDownloadRequestFromRecoveryDetails(
	details []*blobRecoveryMetadata,
	validatedBlobCounts map[common.Hash]uint32,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	expectedColumns map[cltypes.CustodyIndex]bool,
) (*downloadRequest, error) {
	downloadTable := make(map[downloadTableEntry]map[uint64]bool)
	recoveryDetails := make(map[common.Hash]*blobRecoveryMetadata, len(details))
	requestValidatedCounts := make(map[common.Hash]uint32, len(details))
	for _, metadata := range details {
		if metadata == nil {
			continue
		}
		recoveryDetails[metadata.blockRoot] = metadata
		requestValidatedCounts[metadata.blockRoot] = validatedBlobCounts[metadata.blockRoot]

		// get the existing columns from the column storage
		existingColumns, err := columnStorage.GetSavedColumnIndex(context.Background(), metadata.slot, metadata.blockRoot)
		if err != nil {
			return nil, err
		}
		existingColumnsMap := make(map[uint64]bool)
		for _, column := range existingColumns {
			existingColumnsMap[column] = true
		}

		if _, ok := downloadTable[downloadTableEntry{
			blockRoot: metadata.blockRoot,
			slot:      metadata.slot,
		}]; !ok {
			table := make(map[uint64]bool)
			for column := range expectedColumns {
				if !existingColumnsMap[column] {
					table[column] = true
				}
			}
			if len(table) > 0 {
				downloadTable[downloadTableEntry{
					blockRoot: metadata.blockRoot,
					slot:      metadata.slot,
				}] = table
			}
		}
	}
	return &downloadRequest{
		beaconConfig:       beaconConfig,
		downloadTable:      downloadTable,
		recoveryDetails:    recoveryDetails,
		validatedBlobCount: requestValidatedCounts,
	}, nil
}

func (d *downloadRequest) recovery(blockRoot common.Hash) *blobRecoveryMetadata {
	return d.recoveryDetails[blockRoot]
}

func (d *downloadRequest) blobCountNeedsValidation(blockRoot common.Hash, count uint32) bool {
	d.tableMutex.RLock()
	defer d.tableMutex.RUnlock()
	return d.validatedBlobCount[blockRoot] != count
}

func (d *downloadRequest) updateValidatedBlobCount(blockRoot common.Hash, count uint32) {
	d.tableMutex.Lock()
	defer d.tableMutex.Unlock()
	d.validatedBlobCount[blockRoot] = count
}

func (d *downloadRequest) remainingEntries() []downloadTableEntry {
	d.tableMutex.RLock()
	defer d.tableMutex.RUnlock()
	remaining := make([]downloadTableEntry, 0, len(d.downloadTable))
	for entry := range d.downloadTable {
		remaining = append(remaining, entry)
	}
	return remaining
}

func (d *downloadRequest) remainingEntriesCount() int {
	d.tableMutex.RLock()
	defer d.tableMutex.RUnlock()
	return len(d.downloadTable)
}

func (d *downloadRequest) removeColumn(slot uint64, blockRoot common.Hash, columnIndex uint64) {
	d.tableMutex.Lock()
	defer d.tableMutex.Unlock()
	entry := downloadTableEntry{
		blockRoot: blockRoot,
		slot:      slot,
	}
	delete(d.downloadTable[entry], columnIndex)
	if len(d.downloadTable[entry]) == 0 {
		delete(d.downloadTable, entry)
	}
}

func (d *downloadRequest) removeBlock(slot uint64, blockRoot common.Hash) {
	d.tableMutex.Lock()
	defer d.tableMutex.Unlock()
	delete(d.downloadTable, downloadTableEntry{
		blockRoot: blockRoot,
		slot:      slot,
	})
}

func (d *downloadRequest) requestData() (*solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier], map[requestedDataColumn]struct{}) {
	payload := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestBlocksDeneb))
	requested := make(map[requestedDataColumn]struct{})

	d.tableMutex.RLock()
	defer d.tableMutex.RUnlock()
	for entry, columns := range d.downloadTable {
		id := &cltypes.DataColumnsByRootIdentifier{
			BlockRoot: entry.blockRoot,
			Columns:   solid.NewUint64ListSSZ(int(d.beaconConfig.NumberOfColumns)),
		}
		for column := range columns {
			id.Columns.Append(column)
			requested[requestedDataColumn{slot: entry.slot, blockRoot: entry.blockRoot, index: column}] = struct{}{}
		}
		if id.Columns.Length() > 0 {
			payload.Append(id)
		}
	}
	return payload, requested
}

func (d *peerdas) SyncColumnDataLater(block *cltypes.SignedBeaconBlock) error {
	if block.Version() < clparams.FuluVersion {
		return nil
	}
	// [Modified in Gloas:EIP7732] Use GetBlobKzgCommitments() which is version-aware
	// For GLOAS, commitments are in SignedExecutionPayloadBid.Message
	kzgCommitments := block.GetBlobKzgCommitments()
	if kzgCommitments == nil || kzgCommitments.Len() == 0 {
		return nil
	}
	blockRoot, err := block.BlockHashSSZ()
	if err != nil {
		return err
	}
	// [Modified in Gloas:EIP7732] Store SignedBeaconBlock directly via ColumnSyncableSignedBlock interface
	// instead of calling Blinded() which fails for GLOAS blocks
	d.blocksToCheckSync.Store(common.Hash(blockRoot), block)
	return nil
}

func (d *peerdas) syncColumnDataWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// check peers count
			if d.rpc != nil {
				if peersCount, err := d.rpc.Peers(); err != nil {
					log.Warn("failed to get peers count", "err", err)
					continue
				} else if peersCount == 0 {
					log.Info("[syncColumnDataWorker] no peers available, skipping sync")
					continue
				}
			}

			// [Modified in Gloas:EIP7732] Use ColumnSyncableSignedBlock interface
			blocks := []cltypes.ColumnSyncableSignedBlock{}
			roots := []common.Hash{}
			d.blocksToCheckSync.Range(func(key, value any) bool {
				root := key.(common.Hash)
				block := value.(cltypes.ColumnSyncableSignedBlock)
				curSlot := d.ethClock.GetCurrentSlot()
				if curSlot-block.GetSlot() < 5 { // wait slow data from peers
					// skip blocks that are too close to the current slot
					return true
				}
				available, err := d.IsDataAvailable(block.GetSlot(), root)
				switch {
				case err != nil:
					log.Warn("failed to check if data is available", "err", err)
				case available:
					log.Trace("[syncColumnDataWorker] column data is already available, removing from sync queue", "slot", block.GetSlot(), "blockRoot", root)
					d.blocksToCheckSync.Delete(root)
				default:
					blocks = append(blocks, block)
					roots = append(roots, root)
				}
				return true
			})
			if len(blocks) == 0 {
				continue
			}
			log.Debug("[syncColumnDataWorker] syncing column data", "blocks_count", len(blocks))
			if d.IsArchivedMode() {
				if err := d.DownloadColumnsAndRecoverBlobs(ctx, blocks); err != nil {
					log.Warn("failed to download columns and recover blobs", "err", err)
					continue
				}
			} else {
				if err := d.DownloadOnlyCustodyColumns(ctx, blocks); err != nil {
					log.Warn("failed to download only custody columns", "err", err)
					continue
				}
			}
			for i, root := range roots {
				d.blocksToCheckSync.Delete(root)
				log.Debug("[syncColumnDataWorker] column data is synced, removing from sync queue", "slot", blocks[i].GetSlot(), "blockRoot", root)
			}
		}
	}
}
