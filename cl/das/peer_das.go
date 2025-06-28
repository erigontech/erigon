package das

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/kzg"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/p2p/enode"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
)

//go:generate mockgen -typed=true -destination=mock_services/peer_das_mock.go -package=mock_services . PeerDas
type PeerDas interface {
	DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error
	IsDataAvailable(ctx context.Context, blockRoot common.Hash) (bool, error)
	Prune(keepSlotDistance uint64) error
	UpdateValidatorsCustody(cgc uint64)
	TryScheduleRecover(slot uint64, blockRoot common.Hash) error
	IsBlobAlreadyRecovered(blockRoot common.Hash) bool
	IsColumnOverHalf(blockRoot common.Hash) bool
	StateReader() peerdasstate.PeerDasStateReader
}

var (
	numOfBlobRecoveryWorkers = 4
)

type peerdas struct {
	state         *peerdasstate.PeerDasState
	nodeID        enode.ID
	rpc           *rpc.BeaconRpcP2P
	beaconConfig  *clparams.BeaconChainConfig
	columnStorage blob_storage.DataColumnStorage
	blobStorage   blob_storage.BlobStorage
	sentinel      sentinelproto.SentinelClient
	ethClock      eth_clock.EthereumClock
	recoverBlobs  chan recoverBlobsRequest

	recoveringMutex sync.Mutex
	isRecovering    map[common.Hash]bool
}

func NewPeerDas(
	ctx context.Context,
	rpc *rpc.BeaconRpcP2P,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	blobStorage blob_storage.BlobStorage,
	sentinel sentinelproto.SentinelClient,
	nodeID enode.ID,
	ethClock eth_clock.EthereumClock,
	peerDasState *peerdasstate.PeerDasState,
) PeerDas {
	kzg.InitKZG()
	p := &peerdas{
		state:         peerDasState,
		nodeID:        nodeID,
		rpc:           rpc,
		beaconConfig:  beaconConfig,
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
		sentinel:      sentinel,
		ethClock:      ethClock,
		recoverBlobs:  make(chan recoverBlobsRequest, 32),

		recoveringMutex: sync.Mutex{},
		isRecovering:    make(map[common.Hash]bool),
	}
	for range numOfBlobRecoveryWorkers {
		go p.blobsRecoverWorker(ctx)
	}
	return p
}

func (d *peerdas) StateReader() peerdasstate.PeerDasStateReader {
	return d.state
}

func (d *peerdas) IsBlobAlreadyRecovered(blockRoot common.Hash) bool {
	count, err := d.blobStorage.KzgCommitmentsCount(context.Background(), blockRoot)
	if err != nil {
		log.Warn("failed to get kzg commitments count", "err", err, "blockRoot", blockRoot)
		return false
	}
	return count > 0
}

func (d *peerdas) IsColumnOverHalf(blockRoot common.Hash) bool {
	existingColumns, err := d.columnStorage.GetSavedColumnIndex(context.Background(), blockRoot)
	if err != nil {
		log.Warn("failed to get saved column index", "err", err, "blockRoot", blockRoot)
		return false
	}
	return len(existingColumns) >= int(d.beaconConfig.NumberOfColumns+1)/2
}

type recoverBlobsRequest struct {
	slot      uint64
	blockRoot common.Hash
}

/*
func (d *peerdas) p2pTopicsControl(ctx context.Context) {
	// TODO: check if it's upgraded to fulu by notification
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// check if it's upgraded to fulu
		}
	}
}*/

func (d *peerdas) IsDataAvailable(ctx context.Context, blockRoot common.Hash) (bool, error) {
	existingColumns, err := d.columnStorage.GetSavedColumnIndex(ctx, blockRoot)
	if err != nil {
		return false, err
	}

	custodyColumns, err := d.state.GetMyCustodyColumns()
	if err != nil {
		return false, err
	}

	for _, column := range existingColumns {
		delete(custodyColumns, column)
	}

	return len(custodyColumns) == 0, nil
}

func (d *peerdas) UpdateValidatorsCustody(cgc uint64) {
	adCgcChanged := d.state.SetCustodyGroupCount(cgc)
	if adCgcChanged {
		// advertise cgc changed
		// try backfill the missing columns
	}
}

func (d *peerdas) Prune(keepSlotDistance uint64) error {
	d.columnStorage.Prune(keepSlotDistance)
	earliestSlot := d.ethClock.GetCurrentSlot() - keepSlotDistance
	d.state.SetEarliestAvailableSlot(earliestSlot)
	return nil
}

func (d *peerdas) blobsRecoverWorker(ctx context.Context) {
	recover := func(toRecover recoverBlobsRequest) {
		log.Debug("[blobsRecover] recovering blobs", "slot", toRecover.slot, "blockRoot", toRecover.blockRoot)
		ctx := context.Background()
		slot, blockRoot := toRecover.slot, toRecover.blockRoot
		existingColumns, err := d.columnStorage.GetSavedColumnIndex(ctx, blockRoot)
		if err != nil {
			log.Debug("[blobsRecover] failed to get saved column index", "err", err)
			return
		}
		if len(existingColumns) < int(d.beaconConfig.NumberOfColumns+1)/2 {
			log.Debug("[blobsRecover] not enough columns to recover", "slot", slot, "blockRoot", blockRoot, "existingColumns", len(existingColumns))
			return
		}

		// Recover the matrix from the column sidecars
		matrixEntries := []cltypes.MatrixEntry{}
		var anyColumnSidecar *cltypes.DataColumnSidecar
		for _, columnIndex := range existingColumns {
			sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Debug("[blobsRecover] failed to read column sidecar", "err", err)
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
		numberOfBlobs := uint64(anyColumnSidecar.Column.Len())
		blobMatrix, err := peerdasutils.RecoverMatrix(matrixEntries, numberOfBlobs)
		if err != nil {
			log.Warn("[blobsRecover] failed to recover matrix", "err", err, "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)
			return
		}
		log.Debug("[blobsRecover] recovered matrix", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)

		// Recover blobs from the matrix
		blobSidecars := make([]*cltypes.BlobSidecar, 0, len(blobMatrix))
		for blobIndex, blobEntries := range blobMatrix {
			var (
				blob           cltypes.Blob
				kzgCommitment  common.Bytes48
				kzgProof       common.Bytes48
				inclusionProof solid.HashVectorSSZ = solid.NewHashVector(cltypes.KzgCommitmentsInclusionProofDepth) // TODO
			)
			// blob
			for i := range len(blobEntries) / 2 {
				if copied := copy(blob[i*cltypes.BytesPerCell:], blobEntries[i].Cell[:]); copied != cltypes.BytesPerCell {
					log.Warn("[blobsRecover] failed to copy cell", "blobIndex", blobIndex, "slot", slot, "blockRoot", blockRoot)
					return
				}
			}
			// kzg commitment
			copy(kzgCommitment[:], anyColumnSidecar.KzgCommitments.Get(blobIndex)[:])
			// kzg proof
			ckzgBlob := ckzg.Blob(blob)
			proof, err := ckzg.ComputeBlobKZGProof(&ckzgBlob, ckzg.Bytes48(kzgCommitment))
			if err != nil {
				log.Warn("[blobsRecover] failed to compute blob kzg proof", "blobIndex", blobIndex, "slot", slot, "blockRoot", blockRoot)
				return
			}
			copy(kzgProof[:], proof[:])
			blobSidecar := cltypes.NewBlobSidecar(
				uint64(blobIndex),
				&blob,
				kzgCommitment,
				kzgProof,
				anyColumnSidecar.SignedBlockHeader,
				inclusionProof)
			blobSidecars = append(blobSidecars, blobSidecar)
		}

		// Save blobs
		if err := d.blobStorage.WriteBlobSidecars(ctx, blockRoot, blobSidecars); err != nil {
			log.Warn("[blobsRecover] failed to write blob sidecars", "err", err, "slot", slot, "blockRoot", blockRoot)
			return
		}
		log.Debug("[blobsRecover] saved blobs", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)

		// remove column sidecars that are not in our custody group
		custodyColumns, err := d.state.GetMyCustodyColumns()
		if err != nil {
			log.Warn("[blobsRecover] failed to get my custody columns", "err", err, "slot", slot, "blockRoot", blockRoot)
			return
		}
		for _, column := range existingColumns {
			if _, ok := custodyColumns[cltypes.CustodyIndex(column)]; !ok {
				if err := d.columnStorage.RemoveColumnSidecar(ctx, slot, blockRoot, int64(column)); err != nil {
					log.Warn("[blobsRecover] failed to remove column sidecar", "err", err, "slot", slot, "blockRoot", blockRoot, "column", column)
				}
			}
		}
		log.Debug("[blobsRecover] removed column sidecars", "slot", slot, "blockRoot", blockRoot, "numberOfColumns", len(existingColumns))
	}

	// main loop
	for {
		select {
		case <-ctx.Done():
			return
		case toRecover := <-d.recoverBlobs:
			d.recoveringMutex.Lock()
			if _, ok := d.isRecovering[toRecover.blockRoot]; ok {
				// recovering, skip
				d.recoveringMutex.Unlock()
				continue
			}
			d.isRecovering[toRecover.blockRoot] = true
			d.recoveringMutex.Unlock()

			// check if the blobs are already recovered
			if d.IsBlobAlreadyRecovered(toRecover.blockRoot) {
				// already recovered, skip
				d.recoveringMutex.Lock()
				delete(d.isRecovering, toRecover.blockRoot)
				d.recoveringMutex.Unlock()
				continue
			}

			// recover the blobs
			recover(toRecover)

			// remove the block from the recovering map
			d.recoveringMutex.Lock()
			delete(d.isRecovering, toRecover.blockRoot)
			d.recoveringMutex.Unlock()
		}
	}
}

func (d *peerdas) TryScheduleRecover(slot uint64, blockRoot common.Hash) error {
	if d.IsColumnOverHalf(blockRoot) || d.IsBlobAlreadyRecovered(blockRoot) {
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
	log.Debug("[blobsRecover] scheduling recover", "slot", slot, "blockRoot", blockRoot)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case d.recoverBlobs <- recoverBlobsRequest{
		slot:      slot,
		blockRoot: blockRoot,
	}:
	case <-timer.C:
		return errors.New("failed to schedule recover: timeout")
	}
	return nil
}

func (d *peerdas) DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error {
	begin := time.Now()
	defer func() {
		slots := []uint64{}
		for _, block := range blocks {
			slots = append(slots, block.Block.Slot)
		}
		log.Debug("DownloadColumnsAndRecoverBlobs", "time", time.Since(begin), "slots", slots)
	}()

	// filter out blocks that don't need to be processed
	blocksToProcess := []*cltypes.SignedBeaconBlock{}
	for _, block := range blocks {
		if block.Version() < clparams.FuluVersion ||
			block.Block.Body.BlobKzgCommitments == nil ||
			block.Block.Body.BlobKzgCommitments.Len() == 0 {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err != nil {
			log.Warn("failed to get block root", "err", err)
			continue
		}
		if d.IsColumnOverHalf(root) || d.IsBlobAlreadyRecovered(root) {
			continue
		}
		blocksToProcess = append(blocksToProcess, block)
	}

	if len(blocksToProcess) == 0 {
		return nil
	}

	// initialize the download request
	req, err := initializeDownloadRequest(blocksToProcess, d.beaconConfig, d.columnStorage)
	if err != nil {
		return err
	}

	type resultData struct {
		sidecars []*cltypes.DataColumnSidecar
		pid      string
		err      error
	}

	stopChan := make(chan struct{})
	defer close(stopChan)
	resultChan := make(chan resultData, 64)
	requestColumnSidecars := func(req *downloadRequest) {
		// send the request in a loop with a ticker to avoid overwhelming the peer
		// keep trying until the request is done
		ticker := time.NewTicker(50 * time.Millisecond)
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
					ids := req.requestData()
					if ids.Len() == 0 {
						return
					}
					s, pid, err := d.rpc.SendColumnSidecarsByRootIdentifierReq(cctx, ids)
					cancel()
					select {
					case resultChan <- resultData{
						sidecars: s,
						pid:      pid,
						err:      err,
					}:
					default:
						// just drop it if the channel is full
					}
				}()
			}
		}
		wg.Wait()
		close(resultChan)
	}

	// send the request
	go requestColumnSidecars(req)

mainloop:
	for {
		select {
		case <-ctx.Done():
			break mainloop
		case result := <-resultChan:
			if result.err != nil {
				log.Debug("failed to download columns from peer", "pid", result.pid, "err", result.err)
				//d.rpc.BanPeer(result.pid)
				continue
			}
			if len(result.sidecars) == 0 {
				continue
			}

			wg := sync.WaitGroup{}
			for _, sidecar := range result.sidecars {
				wg.Add(1)
				go func(sidecar *cltypes.DataColumnSidecar) {
					defer wg.Done()
					blockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
					if err != nil {
						log.Debug("failed to get block root", "err", err)
						d.rpc.BanPeer(result.pid)
						return
					}
					slot := sidecar.SignedBlockHeader.Header.Slot
					defer func() {
						// check if need to schedule recover whenever we download a column sidecar
						if d.IsColumnOverHalf(blockRoot) {
							req.removeBlock(blockRoot)
							if err := d.TryScheduleRecover(slot, blockRoot); err != nil {
								log.Warn("failed to schedule recover", "err", err, "slot", slot, "blockRoot", blockRoot)
							}
						}
					}()

					columnIndex := sidecar.Index
					columnData := sidecar
					exist, err := d.columnStorage.ColumnSidecarExists(ctx, sidecar.SignedBlockHeader.Header.Slot, blockRoot, int64(columnIndex))
					if err != nil {
						log.Debug("failed to check if column sidecar exists", "err", err)
						d.rpc.BanPeer(result.pid)
						return
					}
					if exist {
						req.removeColumn(blockRoot, columnIndex)
						return
					}

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
					// save the sidecar to the column storage
					if err := d.columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(columnIndex), columnData); err != nil {
						log.Debug("failed to write column sidecar", "err", err)
						return
					}
					// done. remove the column from the download table
					req.removeColumn(blockRoot, columnIndex)
				}(sidecar)
			}
			wg.Wait()
			// check if there are any remaining requests and send again if there are
			if req.requestData().Len() == 0 {
				break mainloop
			}
		}
	}

	return nil
}

// getExpectedColumnIndex returns expected column indexes for the given block root
// Note that expected_columns is subset of custody_columns
func (d *peerdas) getExpectedColumnIndex(
	ctx context.Context,
	blockRoot common.Hash,
	custodyColumns map[cltypes.CustodyIndex]bool,
) ([]uint64, error) {
	existingColumns, err := d.columnStorage.GetSavedColumnIndex(ctx, blockRoot)
	if err != nil {
		return nil, err
	}

	existingColumnsMap := make(map[uint64]bool)
	for _, column := range existingColumns {
		existingColumnsMap[column] = true
	}

	// expected_colums = custody_columns - existing_columns
	want := make([]uint64, 0)
	for c := range custodyColumns {
		if !existingColumnsMap[c] {
			want = append(want, c)
		}
	}
	sort.Slice(want, func(i, j int) bool {
		return want[i] < want[j]
	})

	// TODO: Consider data recovery when having more than 50% of the columns
	// eg: we can just collect 50% of the columns and then recover the rest
	return want, nil
}

// downloadRequest is used to track the download progress of the column sidecars
type downloadRequest struct {
	beaconConfig           *clparams.BeaconChainConfig
	mutex                  sync.RWMutex
	blockRootToBeaconBlock map[common.Hash]*cltypes.SignedBeaconBlock
	downloadTable          map[common.Hash]map[uint64]bool
	cacheRequest           *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]
}

func initializeDownloadRequest(blocks []*cltypes.SignedBeaconBlock, beaconConfig *clparams.BeaconChainConfig, columnStorage blob_storage.DataColumnStorage) (*downloadRequest, error) {
	downloadTable := make(map[common.Hash]map[uint64]bool)
	blockRootToBeaconBlock := make(map[common.Hash]*cltypes.SignedBeaconBlock)
	for _, block := range blocks {
		if block.Version() < clparams.FuluVersion {
			continue
		}
		if block.Block.Body.BlobKzgCommitments.Len() == 0 {
			continue
		}

		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		blockRootToBeaconBlock[blockRoot] = block

		// get the existing columns from the column storage
		existingColumns, err := columnStorage.GetSavedColumnIndex(context.Background(), blockRoot)
		if err != nil {
			return nil, err
		}
		existingColumnsMap := make(map[uint64]bool)
		for _, column := range existingColumns {
			existingColumnsMap[column] = true
		}

		if _, ok := downloadTable[blockRoot]; !ok {
			table := make(map[uint64]bool)
			for i := range beaconConfig.NumberOfColumns { // try download all columns for now
				if !existingColumnsMap[i] {
					table[i] = true
				}
			}
			downloadTable[blockRoot] = table
		}
	}
	return &downloadRequest{
		beaconConfig:           beaconConfig,
		downloadTable:          downloadTable,
		blockRootToBeaconBlock: blockRootToBeaconBlock,
	}, nil
}

func (d *downloadRequest) removeColumn(blockRoot common.Hash, columnIndex uint64) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.downloadTable[blockRoot], columnIndex)
	if len(d.downloadTable[blockRoot]) == 0 {
		d.removeBlock(blockRoot)
	}
	d.cacheRequest = nil
}

func (d *downloadRequest) removeBlock(blockRoot common.Hash) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.downloadTable, blockRoot)
	d.cacheRequest = nil
}

func (d *downloadRequest) isColumnDataOverHalf(blockRoot common.Hash) bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	toDownload, ok := d.downloadTable[blockRoot]
	if !ok {
		return true // if the block is not in the download table, it means all columns are already downloaded
	}
	return len(toDownload) <= int(d.beaconConfig.NumberOfColumns)/2
}

func (d *downloadRequest) requestData() *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier] {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	if d.cacheRequest != nil {
		return d.cacheRequest
	}
	payload := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestBlocksDeneb))
	for blockRoot, columns := range d.downloadTable {
		id := &cltypes.DataColumnsByRootIdentifier{
			BlockRoot: blockRoot,
			Columns:   solid.NewUint64ListSSZ(int(d.beaconConfig.NumberOfColumns)),
		}
		for column := range columns {
			id.Columns.Append(column)
		}
		if id.Columns.Length() > 0 {
			payload.Append(id)
		}
	}
	d.cacheRequest = payload
	return payload
}
