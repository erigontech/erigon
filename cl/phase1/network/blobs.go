package network

import (
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/net/context"
)

var requestBlobBatchExpiration = 15 * time.Second

// This is just a bunch of functions to handle blobs

// BlobsIdentifiersFromBlocks returns a list of blob identifiers from a list of blocks, which should then be forwarded to the network.
func BlobsIdentifiersFromBlocks(blocks []*cltypes.SignedBeaconBlock) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	for _, block := range blocks {
		if block.Version() < clparams.DenebVersion {
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		kzgCommitments := block.Block.Body.BlobKzgCommitments.Len()
		for i := 0; i < kzgCommitments; i++ {
			ids.Append(&cltypes.BlobIdentifier{
				BlockRoot: blockRoot,
				Index:     uint64(i),
			})
		}
	}
	return ids, nil
}

func BlobsIdentifiersFromBlindedBlocks(blocks []*cltypes.SignedBlindedBeaconBlock) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	for _, block := range blocks {
		if block.Version() < clparams.DenebVersion {
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		kzgCommitments := block.Block.Body.BlobKzgCommitments.Len()
		for i := 0; i < kzgCommitments; i++ {
			ids.Append(&cltypes.BlobIdentifier{
				BlockRoot: blockRoot,
				Index:     uint64(i),
			})
		}
	}
	return ids, nil
}

type PeerAndSidecars struct {
	Peer      string
	Responses []*cltypes.BlobSidecar
}

// RequestBlobsFrantically requests blobs from the network frantically.
func RequestBlobsFrantically(ctx context.Context, r *rpc.BeaconRpcP2P, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
	var atomicResp atomic.Value

	atomicResp.Store(&PeerAndSidecars{})
	reqInterval := time.NewTicker(300 * time.Millisecond)
	defer reqInterval.Stop()
Loop:
	for {
		select {
		case <-reqInterval.C:
			go func() {
				if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
					return
				}
				// this is so we do not get stuck on a side-fork
				responses, pid, err := r.SendBlobsSidecarByIdentifierReq(ctx, req)

				if err != nil {
					return
				}
				if responses == nil {
					return
				}
				if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
					return
				}
				atomicResp.Store(&PeerAndSidecars{
					Peer:      pid,
					Responses: responses,
				})
			}()
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(requestBlobBatchExpiration):
			log.Debug("RequestBlobsFrantically: timeout")
			return nil, nil
		default:
			if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
				break Loop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return atomicResp.Load().(*PeerAndSidecars), nil
}
