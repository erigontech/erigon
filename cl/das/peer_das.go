package das

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/kzg"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/p2p/enode"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
)

//go:generate mockgen -typed=true -destination=mock_services/peer_das_mock.go -package=mock_services . PeerDas
type PeerDas interface {
	DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []*cltypes.SignedBlindedBeaconBlock) error
	DownloadOnlyCustodyColumns(ctx context.Context, blocks []*cltypes.SignedBlindedBeaconBlock) error
	IsDataAvailable(slot uint64, blockRoot common.Hash) (bool, error)
	Prune(keepSlotDistance uint64) error
	UpdateValidatorsCustody(cgc uint64)
	TryScheduleRecover(slot uint64, blockRoot common.Hash) error
	IsBlobAlreadyRecovered(blockRoot common.Hash) bool
	IsColumnOverHalf(slot uint64, blockRoot common.Hash) bool
	IsArchivedMode() bool
	StateReader() peerdasstate.PeerDasStateReader
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
	recoverBlobsQueue chan recoverBlobsRequest

	recoveringMutex sync.Mutex
	isRecovering    map[common.Hash]bool
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
) PeerDas {
	kzg.InitKZG()
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
		recoverBlobsQueue: make(chan recoverBlobsRequest, 32),

		recoveringMutex: sync.Mutex{},
		isRecovering:    make(map[common.Hash]bool),
	}
	p.resubscribeGossip()
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
			if _, err := d.sentinel.SetSubscribeExpiry(context.Background(), &sentinelproto.RequestSubscribeExpiry{
				Topic:          gossip.TopicNameDataColumnSidecar(subnet),
				ExpiryUnixSecs: uint64(time.Unix(0, math.MaxInt64).Unix()),
			}); err != nil {
				log.Warn("[peerdas] failed to set subscribe expiry", "err", err, "subnet", subnet)
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
		if _, err := d.sentinel.SetSubscribeExpiry(context.Background(), &sentinelproto.RequestSubscribeExpiry{
			Topic:          gossip.TopicNameDataColumnSidecar(subnet),
			ExpiryUnixSecs: uint64(time.Unix(0, math.MaxInt64).Unix()),
		}); err != nil {
			log.Warn("[peerdas] failed to set subscribe expiry", "err", err, "column", column, "subnet", subnet)
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
		log.Trace("[blobsRecover] recovering blobs", "slot", toRecover.slot, "blockRoot", toRecover.blockRoot)
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
		log.Trace("[blobsRecover] recovered matrix", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs)

		// Recover blobs from the matrix
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
			commitment := cltypes.KZGCommitment(kzgCommitment)
			blobCommitments.Append(&commitment)
		}
		// inclusion proof
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
		for _, column := range existingColumns {
			if _, ok := custodyColumns[column]; !ok {
				if err := d.columnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, int64(column)); err != nil {
					log.Warn("[blobsRecover] failed to remove column sidecar", "err", err, "slot", slot, "blockRoot", blockRoot, "column", column)
				}
			}
		}
		// add custody data column if it doesn't exist
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
				sidecar.KzgCommitmentsInclusionProof = anyColumnSidecar.KzgCommitmentsInclusionProof
				sidecar.KzgCommitments = anyColumnSidecar.KzgCommitments
				for i := range blobSize {
					// cell
					sidecar.Column.Append(&blobMatrix[i][columnIndex].Cell)
					// kzg proof
					sidecar.KzgProofs.Append(&blobMatrix[i][columnIndex].KzgProof)
				}
				// verify the sidecar
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
				// save the sidecar to the column storage
				if err := d.columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(columnIndex), sidecar); err != nil {
					log.Warn("[blobsRecover] failed to write column sidecar", "err", err, "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
					continue
				}
				log.Debug("[blobsRecover] added a custody data column", "slot", slot, "blockRoot", blockRoot, "column", columnIndex)
			}
		}

		log.Debug("[blobsRecover] recovering done", "slot", slot, "blockRoot", blockRoot, "numberOfBlobs", numberOfBlobs, "elapsedTime", time.Since(begin))
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
	if !d.IsArchivedMode() {
		// only recover blobs in archived mode
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
	log.Debug("[blobsRecover] scheduling recover", "slot", slot, "blockRoot", blockRoot)
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
func (d *peerdas) DownloadOnlyCustodyColumns(ctx context.Context, blocks []*cltypes.SignedBlindedBeaconBlock) error {
	custodyColumns, err := d.state.GetMyCustodyColumns()
	if err != nil {
		return err
	}
	req, err := initializeDownloadRequest(blocks, d.beaconConfig, d.columnStorage, custodyColumns)
	if err != nil {
		return err
	}
	return d.runDownload(ctx, req, false)
}

func (d *peerdas) DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []*cltypes.SignedBlindedBeaconBlock) error {
	// filter out blocks that don't need to be processed
	blocksToProcess := []*cltypes.SignedBlindedBeaconBlock{}
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

		if d.IsColumnOverHalf(block.Block.Slot, root) || d.IsBlobAlreadyRecovered(root) {
			if err := d.TryScheduleRecover(block.Block.Slot, root); err != nil {
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
		slots := []uint64{}
		for _, block := range blocks {
			slots = append(slots, block.Block.Slot)
		}
		log.Debug("DownloadColumnsAndRecoverBlobs", "elapsed time", time.Since(begin), "slots", slots)
	}()

	// initialize the download request
	req, err := initializeDownloadRequest(blocksToProcess, d.beaconConfig, d.columnStorage, allColumns)
	if err != nil {
		return err
	}

	return d.runDownload(ctx, req, true)
}

func (d *peerdas) runDownload(ctx context.Context, req *downloadRequest, needToRecoverBlobs bool) error {
	type resultData struct {
		sidecars  []*cltypes.DataColumnSidecar
		pid       string
		reqLength int
		err       error
	}
	if len(req.remainingEntries()) == 0 {
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
				if needToRecoverBlobs &&
					(d.IsColumnOverHalf(entry.slot, entry.blockRoot) || d.IsBlobAlreadyRecovered(entry.blockRoot)) {
					// no need to schedule recovery for this block because someone else will do it
					req.removeBlock(entry.slot, entry.blockRoot)
				}
			}
			if req.requestData().Len() == 0 {
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
					blockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
					if err != nil {
						log.Debug("failed to get block root", "err", err)
						d.rpc.BanPeer(result.pid)
						return
					}
					slot := sidecar.SignedBlockHeader.Header.Slot
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
					exist, err := d.columnStorage.ColumnSidecarExists(ctx, sidecar.SignedBlockHeader.Header.Slot, blockRoot, int64(columnIndex))
					if err != nil {
						log.Debug("failed to check if column sidecar exists", "err", err)
						d.rpc.BanPeer(result.pid)
						return
					}
					if exist {
						req.removeColumn(slot, blockRoot, columnIndex)
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
					req.removeColumn(slot, blockRoot, columnIndex)
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

type downloadTableEntry struct {
	blockRoot common.Hash
	slot      uint64
}

// downloadRequest is used to track the download progress of the column sidecars
type downloadRequest struct {
	beaconConfig           *clparams.BeaconChainConfig
	mutex                  sync.RWMutex
	blockRootToBeaconBlock map[common.Hash]*cltypes.SignedBlindedBeaconBlock
	downloadTable          map[downloadTableEntry]map[uint64]bool
	cacheRequest           *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]
}

func initializeDownloadRequest(
	blocks []*cltypes.SignedBlindedBeaconBlock,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	expectedColumns map[cltypes.CustodyIndex]bool,
) (*downloadRequest, error) {
	downloadTable := make(map[downloadTableEntry]map[uint64]bool)
	blockRootToBeaconBlock := make(map[common.Hash]*cltypes.SignedBlindedBeaconBlock)
	for _, block := range blocks {
		if block.Version() < clparams.FuluVersion {
			continue
		}
		if block.Block.Body.BlobKzgCommitments == nil || block.Block.Body.BlobKzgCommitments.Len() == 0 {
			continue
		}

		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		blockRootToBeaconBlock[blockRoot] = block

		// get the existing columns from the column storage
		existingColumns, err := columnStorage.GetSavedColumnIndex(context.Background(), block.Block.Slot, blockRoot)
		if err != nil {
			return nil, err
		}
		existingColumnsMap := make(map[uint64]bool)
		for _, column := range existingColumns {
			existingColumnsMap[column] = true
		}

		if _, ok := downloadTable[downloadTableEntry{
			blockRoot: blockRoot,
			slot:      block.Block.Slot,
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
					slot:      block.Block.Slot,
				}] = table
			}
		}
	}
	return &downloadRequest{
		beaconConfig:           beaconConfig,
		downloadTable:          downloadTable,
		blockRootToBeaconBlock: blockRootToBeaconBlock,
	}, nil
}

func (d *downloadRequest) remainingEntries() []downloadTableEntry {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	remaining := []downloadTableEntry{}
	for entry := range d.downloadTable {
		remaining = append(remaining, entry)
	}
	return remaining
}

func (d *downloadRequest) removeColumn(slot uint64, blockRoot common.Hash, columnIndex uint64) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	entry := downloadTableEntry{
		blockRoot: blockRoot,
		slot:      slot,
	}
	delete(d.downloadTable[entry], columnIndex)
	if len(d.downloadTable[entry]) == 0 {
		delete(d.downloadTable, entry)
	}
	d.cacheRequest = nil
}

func (d *downloadRequest) removeBlock(slot uint64, blockRoot common.Hash) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.downloadTable, downloadTableEntry{
		blockRoot: blockRoot,
		slot:      slot,
	})
	d.cacheRequest = nil
}

func (d *downloadRequest) requestData() *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier] {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	if d.cacheRequest != nil {
		return d.cacheRequest
	}
	payload := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestBlocksDeneb))
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
	d.cacheRequest = payload
	return payload
}
