package das

import (
	"context"
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
	"github.com/erigontech/erigon/cl/kzg"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/p2p/enode"
)

//go:generate mockgen -typed=true -destination=mock_services/peer_das_mock.go -package=mock_services . PeerDas
type PeerDas interface {
	peerdasstate.PeerDasStateReader
	DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error
	IsDataAvailable(ctx context.Context, blockRoot common.Hash) (bool, error)
	Prune(keepSlotDistance uint64) error
	UpdateValidatorsCustody(cgc uint64)
	TryScheduleRecover(blockRoot common.Hash) error
}

type peerdas struct {
	peerdasstate.PeerDasStateReader
	state         *peerdasstate.PeerDasState
	nodeID        enode.ID
	rpc           *rpc.BeaconRpcP2P
	beaconConfig  *clparams.BeaconChainConfig
	columnStorage blob_storage.DataColumnStorage
	sentinel      sentinelproto.SentinelClient
	ethClock      eth_clock.EthereumClock
}

func NewPeerDas(
	rpc *rpc.BeaconRpcP2P,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	sentinel sentinelproto.SentinelClient,
	nodeID enode.ID,
	ethClock eth_clock.EthereumClock,
) (PeerDas, peerdasstate.PeerDasStateReader) {
	kzg.InitKZG()
	state := peerdasstate.NewPeerDasState(beaconConfig, nodeID)
	p := &peerdas{
		PeerDasStateReader: state,
		state:              state,
		nodeID:             nodeID,
		rpc:                rpc,
		beaconConfig:       beaconConfig,
		columnStorage:      columnStorage,
		sentinel:           sentinel,
		ethClock:           ethClock,
	}
	return p, state
}

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
}

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

func (d *peerdas) TryScheduleRecover(blockRoot common.Hash) error {
	// TODO: implement data recovery
	return nil
}

type downloadRequest struct {
	beaconConfig  *clparams.BeaconChainConfig
	mutex         sync.RWMutex
	downloadTable map[common.Hash]map[uint64]bool
	cacheRequest  *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]
}

func initializeDownloadRequest(blocks []*cltypes.SignedBeaconBlock, beaconConfig *clparams.BeaconChainConfig) (*downloadRequest, error) {
	downloadTable := make(map[common.Hash]map[uint64]bool)
	for _, block := range blocks {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		if _, ok := downloadTable[blockRoot]; !ok {
			downloadTable[blockRoot] = make(map[uint64]bool)
			for i := range beaconConfig.NumberOfColumns { // try download all columns for now
				downloadTable[blockRoot][uint64(i)] = true
			}
		}
	}
	return &downloadRequest{
		beaconConfig:  beaconConfig,
		downloadTable: downloadTable,
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

func (d *downloadRequest) isDownloadedOverHalf(blockRoot common.Hash) bool {
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

func (d *peerdas) DownloadColumnsAndRecoverBlobs(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error {
	req, err := initializeDownloadRequest(blocks, d.beaconConfig)
	if err != nil {
		return err
	}

	type resultData struct {
		sidecars []*cltypes.DataColumnSidecar
		pid      string
		err      error
	}

	stopChan := make(chan struct{})
	resultChan := make(chan resultData) // no need to buffer
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
					}
				}()
			}
		}
		wg.Wait()
	}

	// send the request
	go requestColumnSidecars(req)

mainloop:
	for {
		select {
		case <-ctx.Done():
			stopChan <- struct{}{}
		case result := <-resultChan:
			if result.err != nil {
				log.Debug("failed to download columns from peer", "pid", result.pid, "err", result.err)
				d.rpc.BanPeer(result.pid)
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
						if req.isDownloadedOverHalf(blockRoot) {
							req.removeBlock(blockRoot)
							d.TryScheduleRecover(blockRoot)
						}
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
					req.removeColumn(blockRoot, columnIndex)
					if req.isDownloadedOverHalf(blockRoot) {
						req.removeBlock(blockRoot)
						d.TryScheduleRecover(blockRoot)
					}
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

/*
func (d *peerdas) DownloadMissingColumnsByBlocks(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error {
	// TODO: not all beacon nodes support this p2p request for now, so temporarily disable it
	return nil

	// try to request columns in batches of 8 blocks in parallel
	/*
		wg := sync.WaitGroup{}
		for i := 0; i < len(blocks); i += 16 {
			request, err := d.composeIdentifierRequest(ctx, blocks[i:min(i+16, len(blocks))])
			if err != nil {
				return err
			}
			if request.Len() == 0 {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				d.downloadFromPeers(ctx, request)
			}(i)
		}
		wg.Wait()
		return nil
}


func (d *peerdas) downloadFromPeers(ctx context.Context, request *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]) error {
	type resultData struct {
		sidecars []*cltypes.DataColumnSidecar
		pid      string
		err      error
	}

	requestMapLock := sync.Mutex{}
	requestMap := map[common.Hash]map[uint64]bool{} // blockRoot -> columnIndex set
	for i := 0; i < request.Len(); i++ {
		req := request.Get(i)
		blockRoot := req.BlockRoot
		if _, ok := requestMap[blockRoot]; !ok {
			requestMap[blockRoot] = make(map[uint64]bool)
		}
		req.Columns.Range(func(index int, column uint64, length int) bool {
			requestMap[blockRoot][column] = true
			return true
		})
	}

	stopChan := make(chan struct{})
	resultChan := make(chan resultData) // no need to buffer
	requestColumnSidecars := func(request *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]) {
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
					s, pid, err := d.rpc.SendColumnSidecarsByRootIdentifierReq(cctx, request)
					cancel()
					select {
					case resultChan <- resultData{
						sidecars: s,
						pid:      pid,
						err:      err,
					}:
					default:
					}
				}()
			}
		}
		wg.Wait()
	}

	// send the request
	go requestColumnSidecars(request)
	// receive the result
mainloop:
	for {
		select {
		case <-ctx.Done():
			stopChan <- struct{}{}
		case result := <-resultChan:
			if result.err != nil {
				log.Debug("failed to download columns from peer", "pid", result.pid, "err", result.err)
				continue
			}
			if len(result.sidecars) == 0 {
				continue
			}
			// stop sending request
			stopChan <- struct{}{}
			// drain the result channel
			results := []resultData{result}
		drainLoop:
			for {
				select {
				case result := <-resultChan:
					results = append(results, result)
				default:
					break drainLoop
				}
			}
			// process the result
			wg := sync.WaitGroup{}
			for _, result := range results {
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
						// check if the sidecar is expected
						requestMapLock.Lock()
						if _, ok := requestMap[blockRoot]; !ok {
							requestMapLock.Unlock()
							return
						}
						if _, ok := requestMap[blockRoot][sidecar.Index]; !ok {
							requestMapLock.Unlock()
							return
						}
						requestMapLock.Unlock()
						columnIndex := sidecar.Index
						columnData := sidecar

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
						requestMapLock.Lock()
						delete(requestMap[blockRoot], sidecar.Index)
						if len(requestMap[blockRoot]) == 0 {
							delete(requestMap, blockRoot)
						}
						requestMapLock.Unlock()
					}(sidecar)
				}
			}
			wg.Wait()
			// check if there are any remaining requests and send again if there are
			r := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestBlocksDeneb))
			for blockRoot, columns := range requestMap {
				id := &cltypes.DataColumnsByRootIdentifier{
					BlockRoot: blockRoot,
					Columns:   solid.NewUint64ListSSZ(int(d.beaconConfig.NumberOfColumns)),
				}
				for column := range columns {
					id.Columns.Append(column)
				}
				if id.Columns.Length() > 0 {
					r.Append(id)
				}
			}
			if r.Len() == 0 {
				break mainloop
			}
			go requestColumnSidecars(r)
		}
	}

	return nil
}

// composeIdentifierRequest composes the request for the column sidecars by root identifier
func (d *peerdas) composeIdentifierRequest(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) (*solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier], error) {
	custodyColumns, err := d.state.GetMyCustodyColumns()
	if err != nil {
		return nil, err
	}

	ids := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestBlocksDeneb))
	for _, block := range blocks {
		if block.Version() < clparams.FuluVersion {
			continue
		}
		if block.Block.Body.BlobKzgCommitments.Len() == 0 {
			// skip the block if it does not have any blob kzg commitments
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		columns, err := d.getExpectedColumnIndex(ctx, blockRoot, custodyColumns)
		if err != nil {
			return nil, err
		}
		id := &cltypes.DataColumnsByRootIdentifier{
			BlockRoot: blockRoot,
			Columns:   solid.NewUint64ListSSZ(int(d.beaconConfig.NumberOfColumns)),
		}
		for _, column := range columns {
			id.Columns.Append(column)
		}
		ids.Append(id)
	}
	return ids, nil
}
*/

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
