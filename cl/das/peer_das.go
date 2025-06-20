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
	"github.com/erigontech/erigon/p2p/enode"
)

//go:generate mockgen -typed=true -destination=mock_services/peer_das_mock.go -package=mock_services . PeerDas
type PeerDas interface {
	peerdasstate.PeerDasStateReader
	DownloadMissingColumnsByBlocks(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error
	IsDataAvailable(ctx context.Context, blockRoot common.Hash) (bool, error)
	DataRecoverAndPrune(ctx context.Context) error
	UpdateValidatorsCustody(cgc uint64)
}

type peerdas struct {
	peerdasstate.PeerDasStateReader
	state         *peerdasstate.PeerDasState
	nodeID        enode.ID
	rpc           *rpc.BeaconRpcP2P
	beaconConfig  *clparams.BeaconChainConfig
	columnStorage blob_storage.DataColumnStorage
	sentinel      sentinelproto.SentinelClient
}

func NewPeerDas(
	rpc *rpc.BeaconRpcP2P,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataColumnStorage,
	sentinel sentinelproto.SentinelClient,
	nodeID enode.ID,
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

func (d *peerdas) DataRecoverAndPrune(ctx context.Context) error {
	// TODO: implement data recovery and pruning
	return nil
}

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
		return nil*/
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
