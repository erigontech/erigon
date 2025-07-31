package rpc

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/p2p/enode"
)

var (
	ErrNoGoodPeer = errors.New("no good peer found")
)

var (
	peersCandidateRefreshInterval = time.Second * 15
)

type columnDataPeers struct {
	sentinel      sentinel.SentinelClient
	beaconConfig  *clparams.BeaconChainConfig
	ethClock      eth_clock.EthereumClock
	peerMetaCache *lru.CacheWithTTL[peerDataKey, *peerData]
	beaconState   *state.CachingBeaconState

	peersMutex sync.RWMutex
	peersQueue []peerData
	peersIndex int
}

func newColumnPeers(
	sentinel sentinel.SentinelClient,
	beaconConfig *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	beaconState *state.CachingBeaconState,
) *columnDataPeers {
	s := &columnDataPeers{
		sentinel:      sentinel,
		beaconConfig:  beaconConfig,
		ethClock:      ethClock,
		peerMetaCache: lru.NewWithTTL[peerDataKey, *peerData]("colum-peer-cache", 512, 5*time.Minute),
		beaconState:   beaconState,
		peersQueue:    []peerData{},
		peersIndex:    0,
	}

	go s.refreshPeers(context.Background())
	return s
}

type peerDataKey struct {
	pid string
}

type peerData struct {
	pid                   string
	mask                  map[uint64]bool
	earliestAvailableSlot uint64
}

func (c *columnDataPeers) refreshPeers(ctx context.Context) {
	run := func() {
		currentVersion := c.ethClock.StateVersionByEpoch(c.ethClock.GetCurrentEpoch())
		if currentVersion < clparams.FuluVersion {
			return
		}
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
			if data, ok := c.peerMetaCache.Get(peerKey); ok {
				newPeers = append(newPeers, *data)
				continue
			}

			// request metadata
			metadata := &cltypes.Metadata{}
			if err := c.simpleReuqest(ctx, pid, communication.MetadataProtocolV3, metadata, []byte{}); err != nil {
				log.Debug("[peerSelector] failed to request peer metadata", "peer", pid, "err", err)
				continue
			}
			if metadata.CustodyGroupCount == nil {
				log.Debug("[peerSelector] empty cgc", "peer", pid)
				continue
			}
			// request status
			buf := new(bytes.Buffer)
			forkDigest, err := c.ethClock.CurrentForkDigest()
			if err != nil {
				log.Debug("[peerSelector] failed to get fork digest", "peer", pid, "err", err)
				continue
			}
			myStatus := &cltypes.Status{
				ForkDigest:            forkDigest,
				FinalizedRoot:         c.beaconState.FinalizedCheckpoint().Root,
				FinalizedEpoch:        c.beaconState.FinalizedCheckpoint().Epoch,
				HeadRoot:              c.beaconState.FinalizedCheckpoint().Root,
				HeadSlot:              c.beaconState.FinalizedCheckpoint().Epoch * c.beaconConfig.SlotsPerEpoch,
				EarliestAvailableSlot: new(uint64),
			}
			if err := ssz_snappy.EncodeAndWrite(buf, myStatus); err != nil {
				log.Debug("[peerSelector] failed to encode my status", "peer", pid, "err", err)
				continue
			}
			status := &cltypes.Status{}
			if err := c.simpleReuqest(ctx, pid, communication.StatusProtocolV2, status, buf.Bytes()); err != nil {
				log.Debug("[peerSelector] failed to request peer status", "peer", pid, "err", err)
				continue
			}
			if status.EarliestAvailableSlot == nil {
				log.Debug("[peerSelector] empty earliest available slot", "peer", pid)
				continue
			}

			// get custody indices
			enodeId := enode.HexID(peer.EnodeId)
			custodyIndices, err := peerdasutils.GetCustodyColumns(enodeId, *metadata.CustodyGroupCount)
			if err != nil {
				log.Debug("[peerSelector] failed to get custody indices", "peer", pid, "err", err)
				continue
			}
			data := &peerData{pid: pid, mask: custodyIndices, earliestAvailableSlot: *status.EarliestAvailableSlot}
			c.peerMetaCache.Add(peerKey, data)
			newPeers = append(newPeers, *data)
			log.Debug("[peerSelector] added peer", "peer", pid, "custodies", len(custodyIndices), "earliestAvailableSlot", *status.EarliestAvailableSlot)
		}
		c.peersMutex.Lock()
		c.peersQueue = newPeers
		c.peersIndex = 0
		c.peersMutex.Unlock()
		custodies := []uint64{}
		for _, peer := range newPeers {
			custodies = append(custodies, uint64(len(peer.mask)))
		}
		log.Debug("[peerSelector] updated peers", "totalPeers", len(peers.Peers), "peerCount", len(newPeers), "custodies", custodies, "elapsedTime", time.Since(begin))
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

func (c *columnDataPeers) simpleReuqest(ctx context.Context, pid string, topic string, respContainer ssz.EncodableSSZ, payload []byte) error {
	resp, err := c.sentinel.SendPeerRequest(ctx, &sentinel.RequestDataWithPeer{
		Pid:   pid,
		Data:  payload,
		Topic: topic,
	})
	if err != nil {
		return err
	}
	rawData := resp.GetData()
	if err := ssz_snappy.DecodeAndReadNoForkDigest(bytes.NewReader(rawData), respContainer, clparams.FuluVersion); err != nil {
		return err
	}
	return nil
}

func (c *columnDataPeers) availablePeerCount() int {
	c.peersMutex.RLock()
	defer c.peersMutex.RUnlock()
	return len(c.peersQueue)
}

func (c *columnDataPeers) pickPeerRoundRobin(
	ctx context.Context,
	req *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier],
) (*solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier], string, uint64, error) {
	c.peersMutex.Lock()
	defer c.peersMutex.Unlock()

	for range len(c.peersQueue) {
		c.peersIndex = (c.peersIndex + 1) % len(c.peersQueue)
		peer := c.peersQueue[c.peersIndex]
		// matching
		newReq := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](req.Len())
		req.Range(func(_ int, item *cltypes.DataColumnsByRootIdentifier, length int) bool {
			/*if item.Slot < peer.earliestAvailableSlot { // earlist available slot is not reliable now
				//log.Debug("skipping peer", "peer", peer.pid, "slot", item.Slot, "earliestAvailableSlot", peer.earliestAvailableSlot)
				return true
			}*/
			if len(peer.mask) == int(c.beaconConfig.NumberOfColumns) {
				// full mask, no need to filter
				newReq.Append(item)
			} else {
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
			}
			return true
		})
		if newReq.Len() == 0 {
			// no matching columns
			continue
		}
		return newReq, peer.pid, uint64(len(peer.mask)), nil
	}

	log.Trace("no good peer found", "peerCount", len(c.peersQueue))
	return nil, "", 0, ErrNoGoodPeer
}
