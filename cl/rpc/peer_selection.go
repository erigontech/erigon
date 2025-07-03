package rpc

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/p2p/enode"
)

var (
	peersCandidateRefreshInterval = time.Second * 15
)

type columnDataPeers struct {
	sentinel     sentinel.SentinelClient
	beaconConfig *clparams.BeaconChainConfig
	ethClock     eth_clock.EthereumClock
	peerCache    *lru.CacheWithTTL[peerDataKey, *peerData]

	peersMutex sync.RWMutex
	peersQueue []peerData
	peersIndex int
}

func newColumnPeers(
	sentinel sentinel.SentinelClient,
	beaconConfig *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
) *columnDataPeers {
	s := &columnDataPeers{
		sentinel:     sentinel,
		beaconConfig: beaconConfig,
		ethClock:     ethClock,
		peerCache:    lru.NewWithTTL[peerDataKey, *peerData]("colum-peer-cache", 512, 5*time.Minute),
		peersQueue:   []peerData{},
		peersIndex:   0,
	}
	go s.refreshPeers(context.Background())
	return s
}

type peerDataKey struct {
	pid string
}

type peerData struct {
	pid  string
	mask map[uint64]bool
}

func (c *columnDataPeers) refreshPeers(ctx context.Context) {
	run := func() {
		begin := time.Now()
		state := "connected"
		peers, err := c.sentinel.PeersInfo(ctx, &sentinel.PeersInfoRequest{
			State: &state,
		})
		if err != nil {
			log.Warn("[peerSelector] failed to get peers info", "err", err)
			return
		}
		var newPeers []peerData
		for _, peer := range peers.Peers {
			pid := peer.Pid
			peerKey := peerDataKey{pid: pid}
			if data, ok := c.peerCache.Get(peerKey); ok {
				newPeers = append(newPeers, *data)
				continue
			}

			// request metadata
			topic := communication.MetadataProtocolV3
			resp, err := c.sentinel.SendPeerRequest(ctx, &sentinel.RequestDataWithPeer{
				Pid:   pid,
				Data:  []byte{},
				Topic: topic,
			})
			if err != nil {
				log.Debug("[peerSelector] failed to request peer metadata", "peer", pid, "err", err)
				continue
			}
			rawData := resp.GetData()
			metadata := &cltypes.Metadata{}
			if err := ssz_snappy.DecodeAndReadNoForkDigest(bytes.NewReader(rawData), metadata, clparams.FuluVersion); err != nil {
				log.Debug("[peerSelector] failed to decode peer metadata", "peer", pid, "err", err)
				continue
			}
			if metadata.CustodyGroupCount == nil {
				log.Debug("[peerSelector] empty cgc", "peer", pid)
				continue
			}

			// get custody indices
			enodeId := enode.HexID(peer.EnodeId)
			custodyIndices, err := peerdasutils.GetCustodyColumns(enodeId, *metadata.CustodyGroupCount)
			if err != nil {
				log.Debug("[peerSelector] failed to get custody indices", "peer", pid, "err", err)
				continue
			}
			data := &peerData{pid: pid, mask: custodyIndices}
			c.peerCache.Add(peerKey, data)
			newPeers = append(newPeers, *data)
		}
		// sort by length of mask in descending order
		sort.Slice(newPeers, func(i, j int) bool {
			return len(newPeers[i].mask) > len(newPeers[j].mask)
		})
		c.peersMutex.Lock()
		c.peersQueue = newPeers
		c.peersIndex = 0
		c.peersMutex.Unlock()
		cgcs := []uint64{}
		for _, peer := range newPeers {
			cgcs = append(cgcs, uint64(len(peer.mask)))
		}
		log.Debug("[peerSelector] updated peers", "peerCount", len(newPeers), "cgcs", cgcs, "elapsedTime", time.Since(begin))
	}

	// begin
	ticker := time.NewTicker(peersCandidateRefreshInterval)
	defer ticker.Stop()
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (c *columnDataPeers) pickPeerRoundRobin(
	ctx context.Context,
	req *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier],
) (*solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier], string, uint64, error) {
	c.peersMutex.RLock()
	defer c.peersMutex.RUnlock()

	for range len(c.peersQueue) {
		c.peersIndex = (c.peersIndex + 1) % len(c.peersQueue)
		peer := c.peersQueue[c.peersIndex]
		if len(peer.mask) == int(c.beaconConfig.NumberOfColumns) {
			// full mask, no need to filter
			return req, peer.pid, uint64(len(peer.mask)), nil
		}
		// matching
		newReq := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(req.Len()))
		req.Range(func(_ int, item *cltypes.DataColumnsByRootIdentifier, length int) bool {
			identifier := cltypes.NewDataColumnsByRootIdentifier()
			item.Columns.Range(func(_ int, column uint64, _ int) bool {
				if peer.mask[column] {
					identifier.Columns.Append(column)
				}
				return true
			})
			if identifier.Columns.Length() > 0 {
				identifier.BlockRoot = item.BlockRoot
				newReq.Append(identifier)
			}
			return true
		})
		if newReq.Len() == 0 {
			// no matching columns
			continue
		}
		return newReq, peer.pid, uint64(len(peer.mask)), nil
	}

	log.Debug("no good peer found", "peerCount", len(c.peersQueue))
	return nil, "", 0, errors.New("no good peer found")
}
