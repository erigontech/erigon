// Copyright 2024 The Erigon Authors
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

package network

import (
	"errors"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/rpc"
)

var ErrTimeout = errors.New("timeout")

var requestBlobBatchExpiration = 15 * time.Second

// This is just a bunch of functions to handle blobs

// BlobsIdentifiersFromBlocks returns a list of blob identifiers from a list of blocks, which should then be forwarded to the network.
func BlobsIdentifiersFromBlocks(blocks []*cltypes.SignedBeaconBlock, cfg *clparams.BeaconChainConfig) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
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
		if ids.Len()+kzgCommitments > cfg.MaxRequestBlobSidecarsByVersion(block.Version()) {
			break
		}
		for i := 0; i < kzgCommitments; i++ {
			ids.Append(&cltypes.BlobIdentifier{
				BlockRoot: blockRoot,
				Index:     uint64(i),
			})
		}
	}
	return ids, nil
}

func BlobsIdentifiersFromBlindedBlocks(blocks []*cltypes.SignedBlindedBeaconBlock, cfg *clparams.BeaconChainConfig) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
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
		if ids.Len()+kzgCommitments > cfg.MaxRequestBlobSidecarsByVersion(block.Version()) {
			break
		}
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
	timer := time.NewTimer(requestBlobBatchExpiration)
	defer timer.Stop()
	reqInterval := time.NewTicker(100 * time.Millisecond)
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
					log.Trace("RequestBlobsFrantically: error", "err", err, "peer", pid)
					return
				}
				if responses == nil {
					log.Trace("RequestBlobsFrantically: response is nil", "peer", pid)
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
		case <-timer.C:
			log.Trace("RequestBlobsFrantically: timeout")
			return nil, ErrTimeout
		default:
			if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
				break Loop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return atomicResp.Load().(*PeerAndSidecars), nil
}
