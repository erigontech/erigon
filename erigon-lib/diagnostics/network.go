package diagnostics

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupNetworkDiagnostics() {
	d.runCollectPeersStatistics()
}

func (d *DiagnosticClient) runCollectPeersStatistics() {
	go func() {
		ctx, ch, cancel := Context[PeerStatisticMsgUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(PeerStatisticMsgUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				if value, ok := d.peersSyncMap.Load(info.PeerID); ok {
					if stats, ok := value.(PeerStatistics); ok {
						if info.Inbound {
							stats.BytesIn += uint64(info.Bytes)
							stats.CapBytesIn[info.MsgCap] += uint64(info.Bytes)
							stats.TypeBytesIn[info.MsgType] += uint64(info.Bytes)
						} else {
							stats.BytesOut += uint64(info.Bytes)
							stats.CapBytesOut[info.MsgCap] += uint64(info.Bytes)
							stats.TypeBytesOut[info.MsgType] += uint64(info.Bytes)
						}

						d.peersSyncMap.Store(info.PeerID, stats)
					} else {
						log.Debug("Failed to cast value to PeerStatistics struct", value)
					}
				} else {
					d.peersSyncMap.Store(info.PeerID, PeerStatistics{
						PeerType:     info.PeerType,
						CapBytesIn:   make(map[string]uint64),
						CapBytesOut:  make(map[string]uint64),
						TypeBytesIn:  make(map[string]uint64),
						TypeBytesOut: make(map[string]uint64),
					})
				}
			}
		}
	}()
}

func (d *DiagnosticClient) Peers() map[string]*PeerStatistics {
	stats := make(map[string]*PeerStatistics)

	d.peersSyncMap.Range(func(key, value interface{}) bool {

		if loadedKey, ok := key.(string); ok {
			if loadedValue, ok := value.(PeerStatistics); ok {
				stats[loadedKey] = &loadedValue
			} else {
				log.Debug("Failed to cast value to PeerStatistics struct", value)
			}
		} else {
			log.Debug("Failed to cast key to string", key)
		}

		return true
	})

	d.PeerDataResetStatistics()

	return stats
}

func (d *DiagnosticClient) PeerDataResetStatistics() {
	d.peersSyncMap.Range(func(key, value interface{}) bool {
		if stats, ok := value.(PeerStatistics); ok {
			stats.BytesIn = 0
			stats.BytesOut = 0
			stats.CapBytesIn = make(map[string]uint64)
			stats.CapBytesOut = make(map[string]uint64)
			stats.TypeBytesIn = make(map[string]uint64)
			stats.TypeBytesOut = make(map[string]uint64)

			d.peersSyncMap.Store(key, stats)
		} else {
			log.Debug("Failed to cast value to PeerStatistics struct", value)
		}

		return true
	})
}
