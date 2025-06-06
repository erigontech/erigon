package das

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/rpc"
)

type PeerDas interface {
	DownloadMissingColumnsByBlocks(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error
	IsDataAvailable(ctx context.Context, blockRoot common.Hash) (bool, error)
	CustodyGroupCount() uint64
}

type peerdas struct {
	nodeId        enode.ID
	rpc           *rpc.BeaconRpcP2P
	beaconConfig  *clparams.BeaconChainConfig
	columnStorage blob_storage.DataCloumnStorage

	// cgc is expected to be dynamic value, which varies with the number of validators connecting to the beacon node
	custodyGroupCount atomic.Uint64
}

func NewPeerDas(
	nodeId enode.ID,
	rpc *rpc.BeaconRpcP2P,
	beaconConfig *clparams.BeaconChainConfig,
	columnStorage blob_storage.DataCloumnStorage) PeerDas {
	p := &peerdas{
		nodeId:            nodeId,
		rpc:               rpc,
		beaconConfig:      beaconConfig,
		columnStorage:     columnStorage,
		custodyGroupCount: atomic.Uint64{},
	}
	p.custodyGroupCount.Store(p.beaconConfig.CustodyRequirement)
	return p
}

func (d *peerdas) CustodyGroupCount() uint64 {
	return d.custodyGroupCount.Load()
}

func (d *peerdas) IsDataAvailable(ctx context.Context, blockRoot common.Hash) (bool, error) {
	existingColumns, err := d.columnStorage.GetExistingColumnIndex(ctx, blockRoot)
	if err != nil {
		return false, err
	}

	custodyColumns, err := d.getCustodyColumns()
	if err != nil {
		return false, err
	}

	for _, column := range existingColumns {
		delete(custodyColumns, column)
	}

	return len(custodyColumns) == 0, nil
}

func (d *peerdas) DownloadMissingColumnsByBlocks(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) error {
	// try to request columns in batches of 8 blocks in parallel
	wg := sync.WaitGroup{}
	for i := 0; i < len(blocks); i += 8 {
		request, err := d.composeIdentifierRequest(ctx, blocks[i:min(i+8, len(blocks))])
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

	requestMap := map[common.Hash]map[uint64]bool{} // blockRoot -> columnIndex set
	for i := 0; i < request.Len(); i++ {
		req := request.Get(i)
		blockRoot := req.BlockRoot
		if _, ok := requestMap[blockRoot]; !ok {
			requestMap[blockRoot] = make(map[uint64]bool)
		}
		for _, column := range req.Columns.List() {
			requestMap[blockRoot][column] = true
		}
	}

	stopChan := make(chan struct{})
	resultChan := make(chan resultData, 8)
	requestColumnSidecars := func(request *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]) {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
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
			}
		}
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
			if len(result.sidecars) != 0 {
				// stop sending request and process the result
				stopChan <- struct{}{}
				for _, sidecar := range result.sidecars {
					blockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
					if err != nil {
						log.Debug("failed to get block root", "err", err)
						d.rpc.BanPeer(result.pid)
						continue
					}
					// check if the sidecar is expected
					if _, ok := requestMap[blockRoot]; !ok {
						log.Debug("received unexpected block root", "blockRoot", blockRoot)
						d.rpc.BanPeer(result.pid)
						continue
					}
					if _, ok := requestMap[blockRoot][uint64(sidecar.Index)]; !ok {
						log.Debug("received unexpected column sidecar", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
						continue
					}
					columnIndex := sidecar.Index
					columnData := sidecar

					if !VerifyDataColumnSidecar(sidecar) {
						log.Debug("failed to verify column sidecar", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
						d.rpc.BanPeer(result.pid)
						continue
					}
					if !VerifyDataColumnSidecarInclusionProof(sidecar) {
						log.Debug("failed to verify column sidecar inclusion proof", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
						d.rpc.BanPeer(result.pid)
						continue
					}
					if !VerifyDataColumnSidecarKZGProofs(sidecar) {
						log.Debug("failed to verify column sidecar kzg proofs", "blockRoot", blockRoot, "columnIndex", sidecar.Index)
						d.rpc.BanPeer(result.pid)
						continue
					}

					if err := d.columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(columnIndex), columnData); err != nil {
						log.Debug("failed to write column sidecar", "err", err)
						continue
					}
					delete(requestMap[blockRoot], uint64(sidecar.Index))
				}
				// check if there are any remaining requests and send again if there are
				r := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestDataColumnSidecars))
				for blockRoot, columns := range requestMap {
					columns := make([]uint64, 0, len(columns))
					for column := range columns {
						columns = append(columns, uint64(column))
					}
					if len(columns) > 0 {
						r.Append(&cltypes.DataColumnsByRootIdentifier{
							BlockRoot: blockRoot,
							Columns:   solid.NewListSSZUint64(columns),
						})
					}
				}
				if r.Len() == 0 {
					break mainloop
				}
				go requestColumnSidecars(r)
			}
		}
	}

	return nil
}

func (d *peerdas) getCustodyColumns() (map[CustodyIndex]bool, error) {
	// TODO: cache the following computations in terms of custody columns
	sampleSize := max(d.beaconConfig.SamplesPerSlot, d.custodyGroupCount.Load())
	groups, err := GetCustodyGroups(d.nodeId, sampleSize)
	if err != nil {
		return nil, err
	}
	// compute all required custody columns
	custodyColumns := map[CustodyIndex]bool{}
	for _, group := range groups {
		columns, err := ComputeColumnsForCustodyGroup(group)
		if err != nil {
			return nil, err
		}
		for _, column := range columns {
			custodyColumns[column] = true
		}
	}
	return custodyColumns, nil
}

// composeIdentifierRequest composes the request for the column sidecars by root identifier
func (d *peerdas) composeIdentifierRequest(ctx context.Context, blocks []*cltypes.SignedBeaconBlock) (*solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier], error) {
	custodyColumns, err := d.getCustodyColumns()
	if err != nil {
		return nil, err
	}

	ids := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(d.beaconConfig.MaxRequestDataColumnSidecars))
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
		columns, err := d.getExpectedColumnIndex(ctx, blockRoot, custodyColumns)
		if err != nil {
			return nil, err
		}
		ids.Append(&cltypes.DataColumnsByRootIdentifier{
			BlockRoot: blockRoot,
			Columns:   solid.NewListSSZUint64(columns),
		})
	}
	return ids, nil
}

// getExpectedColumnIndex returns expected column indexes for the given block root
// Note that expected_columns is subset of custody_columns
func (d *peerdas) getExpectedColumnIndex(
	ctx context.Context,
	blockRoot common.Hash,
	custodyColumns map[CustodyIndex]bool,
) ([]uint64, error) {
	existingColumns, err := d.columnStorage.GetExistingColumnIndex(ctx, blockRoot)
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
		if !existingColumnsMap[uint64(c)] {
			want = append(want, uint64(c))
		}
	}
	sort.Slice(want, func(i, j int) bool {
		return want[i] < want[j]
	})

	// TODO: Consider data recovery when having more than 50% of the columns
	// eg: we can just collect 50% of the columns and then recover the rest
	return want, nil
}
