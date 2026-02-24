package das

import (
	"context"
	"errors"
	"math"
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
	// [Modified in Gloas:EIP7732] Changed from []*SignedBlindedBeaconBlock to []ColumnSyncableSignedBlock
	// to support both pre-GLOAS (blinded) and GLOAS (non-blinded) blocks
	DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error
	DownloadOnlyCustodyColumns(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error
	IsDataAvailable(slot uint64, blockRoot common.Hash) (bool, error)
	Prune(keepSlotDistance uint64) error
	UpdateValidatorsCustody(cgc uint64)
	TryScheduleRecover(slot uint64, blockRoot common.Hash) error
	IsBlobAlreadyRecovered(blockRoot common.Hash) bool
	IsColumnOverHalf(slot uint64, blockRoot common.Hash) bool
	IsArchivedMode() bool
	StateReader() peerdasstate.PeerDasStateReader
	SyncColumnDataLater(block *cltypes.SignedBeaconBlock) error
	SetForkChoice(forkChoice BlockGetter) // [New in Gloas:EIP7732]
}

var (
	numOfBlobRecoveryWorkers = 8
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

	recoveringMutex   sync.Mutex
	isRecovering      map[common.Hash]bool
	blocksToCheckSync sync.Map // blockRoot -> ColumnSyncableSignedBlock (SignedBeaconBlock or SignedBlindedBeaconBlock)

	// [New in Gloas:EIP7732] For fetching blocks to get kzg_commitments
	forkChoice     BlockGetter
	blockReader    freezeblocks.BeaconSnapshotReader
	indiciesDB     kv.RoDB
	gloasDataCache *lru.Cache[common.Hash, *gloasBlockData] // cache for GLOAS block data (~1KB per entry)
}

func NewPeerDas(
	ctx context.Context,
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

		recoveringMutex:   sync.Mutex{},
		isRecovering:      make(map[common.Hash]bool),
		blocksToCheckSync: sync.Map{},

		blockReader:    blockReader,
		indiciesDB:     indiciesDB,
		gloasDataCache: gloasDataCache,
	}
	p.resubscribeGossip()
	for range numOfBlobRecoveryWorkers {
		go p.blobsRecoverWorker(ctx)
	}
	go p.syncColumnDataWorker(ctx)
	return p
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

func (d *peerdas) Prune(keepSlotDistance uint64) error {
	if err := d.columnStorage.Prune(keepSlotDistance); err != nil {
		return err
	}

	curSlot := d.ethClock.GetCurrentSlot()
	if curSlot < keepSlotDistance {
		d.state.SetEarliestAvailableSlot(0)
	} else {
		earliestSlot := curSlot - keepSlotDistance
		if earliestSlot > d.state.GetEarliestAvailableSlot() {
			d.state.SetEarliestAvailableSlot(earliestSlot)
		}
	}
	return nil
}

type recoverBlobsRequest struct {
	slot      uint64
	blockRoot common.Hash
}

func (d *peerdas) blobsRecoverWorker(ctx context.Context) {
	recover := func(toRecover recoverBlobsRequest) {
		begin := time.Now()
		log.Debug("[blobsRecover] recovering blobs", "slot", toRecover.slot, "blockRoot", toRecover.blockRoot)
		ctx := context.Background()
		slot, blockRoot := toRecover.slot, toRecover.blockRoot
		existingColumns, err := d.columnStorage.GetSavedColumnIndex(ctx, slot, blockRoot)
		if err != nil {
			log.Warn("[blobsRecover] failed to get saved column index", "err", err)
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
			sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Debug("[blobsRecover] failed to read column sidecar", "err", err)
				d.columnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, int64(columnIndex))
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
		blobMatrix, err := peerdasutils.RecoverMatrix(matrixEntries, numberOfBlobs)
		if err != nil {
			log.Warn("[blobsRecover] failed to recover matrix", "err", err, "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)
			return
		}
		timeRecoverMatrix := time.Since(beginRecoverMatrix)
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
				inclusionProof)
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
		// Save blobs
		if err := d.blobStorage.WriteBlobSidecars(ctx, blockRoot, blobSidecars); err != nil {
			log.Warn("[blobsRecover] failed to write blob sidecars", "err", err, "slot", slot, "blockRoot", blockRoot)
			return
		}
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
				sidecar := cltypes.NewDataColumnSidecar()
				sidecar.Index = columnIndex
				sidecar.SignedBlockHeader = anyColumnSidecar.SignedBlockHeader
				// [Modified in Gloas:EIP7732] GLOAS sidecars don't have KzgCommitmentsInclusionProof and KzgCommitments
				if !isGloas {
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
	}

	// main loop
	for {
		select {
		case <-ctx.Done():
			return
		case toRecover := <-d.recoverBlobsQueue:
			d.recoveringMutex.Lock()
			if _, ok := d.isRecovering[toRecover.blockRoot]; ok {
				// recovering, skip
				d.recoveringMutex.Unlock()
				continue
			}
			d.isRecovering[toRecover.blockRoot] = true
			d.recoveringMutex.Unlock()

			// check if the blobs are already recovered
			if !d.IsBlobAlreadyRecovered(toRecover.blockRoot) {
				// recover the blobs
				recover(toRecover)
			}
			// remove the block from the recovering map
			d.recoveringMutex.Lock()
			delete(d.isRecovering, toRecover.blockRoot)
			d.recoveringMutex.Unlock()
		}
	}
}

func (d *peerdas) TryScheduleRecover(slot uint64, blockRoot common.Hash) error {
	if !d.IsArchivedMode() && !d.StateReader().IsSupernode() {
		return nil
	}

	if !d.IsColumnOverHalf(slot, blockRoot) || d.IsBlobAlreadyRecovered(blockRoot) {
		// no need to recover if column data is not over 50% or the blobs are already recovered
		return nil
	}

	// early check if the blobs are recovering
	d.recoveringMutex.Lock()
	if _, ok := d.isRecovering[blockRoot]; ok {
		d.recoveringMutex.Unlock()
		return nil
	}
	d.recoveringMutex.Unlock()

	// schedule
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case d.recoverBlobsQueue <- recoverBlobsRequest{
		slot:      slot,
		blockRoot: blockRoot,
	}:
	case <-timer.C:
		return errors.New("failed to schedule recover: timeout")
	}
	return nil
}

var (
	allColumns = func() map[cltypes.CustodyIndex]bool {
		columns := map[cltypes.CustodyIndex]bool{}
		for i := range 128 {
			columns[cltypes.CustodyIndex(i)] = true
		}
		return columns
	}()
)

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
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := initializeDownloadRequest(blocks, d.beaconConfig, d.columnStorage, custodyColumns)
			if err != nil {
				log.Warn("failed to initialize download request", "err", err)
				return
			}
			d.runDownload(ctx, req, false)
		}()
	}
	wg.Wait()
	return nil
}

func (d *peerdas) DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []cltypes.ColumnSyncableSignedBlock) error {
	// filter out blocks that don't need to be processed
	blocksToProcess := []cltypes.ColumnSyncableSignedBlock{}
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

		if d.IsColumnOverHalf(block.GetSlot(), root) || d.IsBlobAlreadyRecovered(root) {
			if err := d.TryScheduleRecover(block.GetSlot(), root); err != nil {
				log.Debug("failed to schedule recover", "err", err)
			}
			continue
		}
		blocksToProcess = append(blocksToProcess, block)
	}

	if len(blocksToProcess) == 0 {
		return nil
	}

	begin := time.Now()
	defer func() {
		slots := make([]uint64, 0, len(blocks))
		for _, block := range blocks {
			slots = append(slots, block.GetSlot())
		}
		log.Debug("DownloadColumnsAndRecoverBlobs", "elapsed time", time.Since(begin), "slots", slots)
	}()

	// initialize the download request
	batchBlcokSize := 4
	wg := sync.WaitGroup{}
	for i := 0; i < len(blocksToProcess); i += batchBlcokSize {
		blocks := blocksToProcess[i:min(i+batchBlcokSize, len(blocksToProcess))]
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := initializeDownloadRequest(blocks, d.beaconConfig, d.columnStorage, allColumns)
			if err != nil {
				log.Warn("failed to initialize download request", "err", err)
				return
			}
			d.runDownload(ctx, req, true)
		}()
	}
	wg.Wait()
	return nil
}

func (d *peerdas) runDownload(ctx context.Context, req *downloadRequest, needToRecoverBlobs bool) error {
	type resultData struct {
		sidecars  []*cltypes.DataColumnSidecar
		pid       string
		reqLength int
		err       error
	}
	if req.remainingEntriesCount() == 0 {
		return nil
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
				wg.Add(1)
				go func() {
					defer wg.Done()
					cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					ids := req.requestData()
					if ids.Len() == 0 {
						return
					}
					reqLength := 0
					ids.Range(func(_ int, id *cltypes.DataColumnsByRootIdentifier, length int) bool {
						reqLength += id.Columns.Length()
						return true
					})
					s, pid, err := d.rpc.SendColumnSidecarsByRootIdentifierReq(cctx, ids)
					select {
					case resultChan <- resultData{
						sidecars:  s,
						pid:       pid,
						reqLength: reqLength,
						err:       err,
					}:
					default:
						// just drop it if the channel is full
					}
				}()
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
					if d.IsColumnOverHalf(entry.slot, entry.blockRoot) || d.IsBlobAlreadyRecovered(entry.blockRoot) {
						// no need to schedule recovery for this block because someone else will do it
						req.removeBlock(entry.slot, entry.blockRoot)
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
				log.Debug("failed to download columns from peer", "pid", result.pid, "err", result.err)
				//d.rpc.BanPeer(result.pid)
				continue
			}
			if len(result.sidecars) == 0 {
				continue
			}
			log.Debug("received column sidecars", "pid", result.pid, "reqLength", result.reqLength, "count", len(result.sidecars))
			wg := sync.WaitGroup{}
			for _, sidecar := range result.sidecars {
				wg.Add(1)
				go func(sidecar *cltypes.DataColumnSidecar) {
					defer wg.Done()
					// [Modified in Gloas:EIP7732] Get slot first, then use epoch-based version detection
					var slot uint64
					if sidecar.SignedBlockHeader != nil && sidecar.SignedBlockHeader.Header != nil {
						slot = sidecar.SignedBlockHeader.Header.Slot
					} else {
						slot = sidecar.Slot
					}
					epoch := slot / d.beaconConfig.SlotsPerEpoch
					isGloasSidecar := d.beaconConfig.GetCurrentStateVersion(epoch) >= clparams.GloasVersion

					var blockRoot common.Hash
					if isGloasSidecar {
						blockRoot = sidecar.BeaconBlockRoot
					} else {
						var err error
						blockRoot, err = sidecar.SignedBlockHeader.Header.HashSSZ()
						if err != nil {
							log.Debug("failed to get block root", "err", err)
							d.rpc.BanPeer(result.pid)
							return
						}
					}
					defer func() {
						// check if need to schedule recover whenever we download a column sidecar
						if needToRecoverBlobs &&
							(d.IsColumnOverHalf(slot, blockRoot) || d.IsBlobAlreadyRecovered(blockRoot)) {
							req.removeBlock(slot, blockRoot)
							d.TryScheduleRecover(slot, blockRoot)
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
				}(sidecar)
			}
			wg.Wait()
			// check if there are any remaining requests and send again if there are
			if req.remainingEntriesCount() == 0 {
				break mainloop
			}
		}
	}

	return nil
}

type downloadTableEntry struct {
	blockRoot common.Hash
	slot      uint64
}

// downloadRequest is used to track the download progress of the column sidecars
type downloadRequest struct {
	beaconConfig  *clparams.BeaconChainConfig
	tableMutex    sync.RWMutex
	downloadTable map[downloadTableEntry]map[uint64]bool
}

// [Modified in Gloas:EIP7732] Changed from []*SignedBlindedBeaconBlock to []ColumnSyncableSignedBlock
func initializeDownloadRequest(
	blocks []cltypes.ColumnSyncableSignedBlock,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	expectedColumns map[cltypes.CustodyIndex]bool,
) (*downloadRequest, error) {
	downloadTable := make(map[downloadTableEntry]map[uint64]bool)
	blockRootToBeaconBlock := make(map[common.Hash]cltypes.ColumnSyncableSignedBlock)
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
		blockRootToBeaconBlock[blockRoot] = block

		// get the existing columns from the column storage
		existingColumns, err := columnStorage.GetSavedColumnIndex(context.Background(), block.GetSlot(), blockRoot)
		if err != nil {
			return nil, err
		}
		existingColumnsMap := make(map[uint64]bool)
		for _, column := range existingColumns {
			existingColumnsMap[column] = true
		}

		if _, ok := downloadTable[downloadTableEntry{
			blockRoot: blockRoot,
			slot:      block.GetSlot(),
		}]; !ok {
			table := make(map[uint64]bool)
			for column := range expectedColumns {
				if !existingColumnsMap[column] {
					table[column] = true
				}
			}
			if len(table) > 0 {
				downloadTable[downloadTableEntry{
					blockRoot: blockRoot,
					slot:      block.GetSlot(),
				}] = table
			}
		}
	}
	return &downloadRequest{
		beaconConfig:  beaconConfig,
		downloadTable: downloadTable,
	}, nil
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

func (d *downloadRequest) requestData() *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier] {
	payload := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestBlocksDeneb))

	d.tableMutex.RLock()
	defer d.tableMutex.RUnlock()
	for entry, columns := range d.downloadTable {
		id := &cltypes.DataColumnsByRootIdentifier{
			BlockRoot: entry.blockRoot,
			Columns:   solid.NewUint64ListSSZ(int(d.beaconConfig.NumberOfColumns)),
		}
		for column := range columns {
			id.Columns.Append(column)
		}
		if id.Columns.Length() > 0 {
			payload.Append(id)
		}
	}
	return payload
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
				if err != nil {
					log.Warn("failed to check if data is available", "err", err)
				} else if available {
					log.Trace("[syncColumnDataWorker] column data is already available, removing from sync queue", "slot", block.GetSlot(), "blockRoot", root)
					d.blocksToCheckSync.Delete(root)
				} else {
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
