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

package diagnostics

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/log/v3"
)

type PeerStats struct {
	peersInfo     *sync.Map
	recordsCount  int
	lastUpdateMap map[string]time.Time
	limit         int
	mu            sync.Mutex
}

func NewPeerStats(peerLimit int) *PeerStats {
	return &PeerStats{
		peersInfo:     &sync.Map{},
		recordsCount:  0,
		lastUpdateMap: make(map[string]time.Time),
		limit:         peerLimit,
	}
}

func (p *PeerStats) AddOrUpdatePeer(peerID string, peerInfo PeerStatisticMsgUpdate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if value, ok := p.peersInfo.Load(peerID); ok {
		p.UpdatePeer(peerID, peerInfo, value)
	} else {
		p.AddPeer(peerID, peerInfo)
		if p.GetPeersCount() > p.limit {
			p.RemovePeersWhichExceedLimit(p.limit)
		}
	}
}

func (p *PeerStats) AddPeer(peerID string, peerInfo PeerStatisticMsgUpdate) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pv := PeerStatisticsFromMsgUpdate(peerInfo, nil)
	p.peersInfo.Store(peerID, pv)
	p.recordsCount++
	p.lastUpdateMap[peerID] = time.Now()
}

func (p *PeerStats) UpdatePeer(peerID string, peerInfo PeerStatisticMsgUpdate, prevValue any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pv := PeerStatisticsFromMsgUpdate(peerInfo, prevValue)

	p.peersInfo.Store(peerID, pv)
	p.lastUpdateMap[peerID] = time.Now()
}

func PeerStatisticsFromMsgUpdate(msg PeerStatisticMsgUpdate, prevValue any) PeerStatistics {
	ps := PeerStatistics{
		PeerType:     msg.PeerType,
		BytesIn:      0,
		BytesOut:     0,
		CapBytesIn:   make(map[string]uint64),
		CapBytesOut:  make(map[string]uint64),
		TypeBytesIn:  make(map[string]uint64),
		TypeBytesOut: make(map[string]uint64),
	}

	if stats, ok := prevValue.(PeerStatistics); ok {
		if msg.Inbound {
			ps.BytesIn = stats.BytesIn + uint64(msg.Bytes)
			ps.CapBytesIn[msg.MsgCap] = stats.CapBytesIn[msg.MsgCap] + uint64(msg.Bytes)
			ps.TypeBytesIn[msg.MsgType] = stats.TypeBytesIn[msg.MsgType] + uint64(msg.Bytes)
		} else {
			ps.BytesOut = stats.BytesOut + uint64(msg.Bytes)
			ps.CapBytesOut[msg.MsgCap] = stats.CapBytesOut[msg.MsgCap] + uint64(msg.Bytes)
			ps.TypeBytesOut[msg.MsgType] = stats.TypeBytesOut[msg.MsgType] + uint64(msg.Bytes)
		}
	} else {
		if msg.Inbound {
			ps.BytesIn += uint64(msg.Bytes)
			ps.CapBytesIn[msg.MsgCap] += uint64(msg.Bytes)
			ps.TypeBytesIn[msg.MsgType] += uint64(msg.Bytes)
		} else {
			ps.BytesOut += uint64(msg.Bytes)
			ps.CapBytesOut[msg.MsgCap] += uint64(msg.Bytes)
			ps.TypeBytesOut[msg.MsgType] += uint64(msg.Bytes)
		}

	}

	return ps
}

func (p *PeerStats) GetPeersCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.recordsCount
}

func (p *PeerStats) GetPeers() map[string]PeerStatistics {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := make(map[string]PeerStatistics)
	p.peersInfo.Range(func(key, value interface{}) bool {
		loadedKey, ok := key.(string)
		if !ok {
			log.Debug("Failed to cast key to string", key)
			return true
		}

		loadedValue, ok := value.(PeerStatistics)
		if !ok {
			log.Debug("Failed to cast value to PeerStatistics struct", value)
			return true
		}

		stats[loadedKey] = loadedValue.Clone()
		return true
	})

	return stats
}

func (p *PeerStats) GetPeerStatistics(peerID string) PeerStatistics {
	p.mu.Lock()
	defer p.mu.Unlock()

	if value, ok := p.peersInfo.Load(peerID); ok {
		if peerStats, ok := value.(PeerStatistics); ok {
			return peerStats.Clone()
		}
	}

	return PeerStatistics{}
}

func (p *PeerStats) GetLastUpdate(peerID string) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	if lastUpdate, ok := p.lastUpdateMap[peerID]; ok {
		return lastUpdate
	}

	return time.Time{}
}

func (p *PeerStats) RemovePeer(peerID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peersInfo.Delete(peerID)
	p.recordsCount--
	delete(p.lastUpdateMap, peerID)
}

type PeerUpdTime struct {
	PeerID string
	Time   time.Time
}

func (p *PeerStats) GetOldestUpdatedPeersWithSize(size int) []PeerUpdTime {
	p.mu.Lock()
	defer p.mu.Unlock()

	timeArray := make([]PeerUpdTime, 0, p.GetPeersCount())
	for k, v := range p.lastUpdateMap {
		timeArray = append(timeArray, PeerUpdTime{k, v})
	}

	sort.Slice(timeArray, func(i, j int) bool {
		return timeArray[i].Time.Before(timeArray[j].Time)
	})

	if len(timeArray) < size {
		return timeArray
	} else {
		return timeArray[:size]
	}
}

func (p *PeerStats) RemovePeersWhichExceedLimit(limit int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	peersToRemove := p.GetPeersCount() - limit
	if peersToRemove > 0 {
		peers := p.GetOldestUpdatedPeersWithSize(peersToRemove)
		for _, peer := range peers {
			p.RemovePeer(peer.PeerID)
		}
	}
}

func (d *DiagnosticClient) setupNetworkDiagnostics(rootCtx context.Context) {
	d.runCollectPeersStatistics(rootCtx)
}

func (d *DiagnosticClient) runCollectPeersStatistics(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[PeerStatisticMsgUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(PeerStatisticMsgUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.peersStats.AddOrUpdatePeer(info.PeerID, info)
			}
		}
	}()
}

func (d *DiagnosticClient) Peers() map[string]PeerStatistics {
	return d.peersStats.GetPeers()
}
