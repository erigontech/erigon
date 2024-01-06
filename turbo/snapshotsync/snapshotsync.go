package snapshotsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type CaplinMode int

const (

	// CaplinModeNone - no caplin mode
	NoCaplin   CaplinMode = 1
	OnlyCaplin CaplinMode = 2
	AlsoCaplin CaplinMode = 3
)

func BuildProtoRequest(downloadRequest []services.DownloadRequest) *proto_downloader.AddRequest {
	req := &proto_downloader.AddRequest{Items: make([]*proto_downloader.AddItem, 0, len(snaptype.BlockSnapshotTypes))}
	for _, r := range downloadRequest {
		if r.Path == "" {
			continue
		}
		if r.TorrentHash != "" {
			req.Items = append(req.Items, &proto_downloader.AddItem{
				TorrentHash: downloadergrpc.String2Proto(r.TorrentHash),
				Path:        r.Path,
			})
		} else {
			req.Items = append(req.Items, &proto_downloader.AddItem{
				Path: r.Path,
			})
		}
	}
	return req
}

// RequestSnapshotsDownload - builds the snapshots download request and downloads them
func RequestSnapshotsDownload(ctx context.Context, downloadRequest []services.DownloadRequest, downloader proto_downloader.DownloaderClient) error {
	// start seed large .seg of large size
	req := BuildProtoRequest(downloadRequest)
	if _, err := downloader.Add(ctx, req); err != nil {
		return err
	}
	return nil
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(ctx context.Context, logPrefix string, histV3 bool, caplin CaplinMode, agg *state.AggregatorV3, tx kv.RwTx, blockReader services.FullBlockReader, cc *chain.Config, snapshotDownloader proto_downloader.DownloaderClient, stagesIdsList []string) error {
	snapshots := blockReader.Snapshots()
	borSnapshots := blockReader.BorSnapshots()
	if blockReader.FreezingCfg().NoDownloader {
		if err := snapshots.ReopenFolder(); err != nil {
			return err
		}
		if cc.Bor != nil {
			if err := borSnapshots.ReopenFolder(); err != nil {
				return err
			}
		}
		return nil
	}

	//Corner cases:
	// - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
	// - Erigon "download once": means restart/upgrade/downgrade must not download files (and will be fast)
	// - After "download once" - Erigon will produce and seed new files

	// send all hashes to the Downloader service
	snapCfg := snapcfg.KnownCfg(cc.ChainName, 0)
	preverifiedBlockSnapshots := snapCfg.Preverified
	downloadRequest := make([]services.DownloadRequest, 0, len(preverifiedBlockSnapshots))

	// build all download requests
	for _, p := range preverifiedBlockSnapshots {
		if !histV3 {
			if strings.HasPrefix(p.Name, "domain") || strings.HasPrefix(p.Name, "history") || strings.HasPrefix(p.Name, "idx") {
				continue
			}
		}
		if caplin == NoCaplin && strings.Contains(p.Name, "beaconblocks") {
			continue
		}
		if caplin == OnlyCaplin && !strings.Contains(p.Name, "beaconblocks") {
			continue
		}

		downloadRequest = append(downloadRequest, services.NewDownloadRequest(p.Name, p.Hash))
	}

	log.Info(fmt.Sprintf("[%s] Fetching torrent files metadata", logPrefix))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := RequestSnapshotsDownload(ctx, downloadRequest, snapshotDownloader); err != nil {
			log.Error(fmt.Sprintf("[%s] call downloader", logPrefix), "err", err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	downloadStartTime := time.Now()
	const logInterval = 20 * time.Second
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var m runtime.MemStats

	// Check once without delay, for faster erigon re-start
	stats, err := snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{})
	if err == nil && stats.Completed {
		goto Finish
	}

	// Print download progress until all segments are available
Loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			if stats, err := snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
				log.Warn("Error while waiting for snapshots progress", "err", err)
			} else if stats.Completed {
				diagnostics.Send(diagnostics.SnapshotDownloadStatistics{
					Downloaded:       stats.BytesCompleted,
					Total:            stats.BytesTotal,
					TotalTime:        time.Since(downloadStartTime).Round(time.Second).Seconds(),
					DownloadRate:     stats.DownloadRate,
					UploadRate:       stats.UploadRate,
					Peers:            stats.PeersUnique,
					Files:            stats.FilesTotal,
					Connections:      stats.ConnectionsTotal,
					Alloc:            m.Alloc,
					Sys:              m.Sys,
					DownloadFinished: stats.Completed,
				})

				log.Info(fmt.Sprintf("[%s] download finished", logPrefix), "time", time.Since(downloadStartTime).String())
				break Loop
			} else {
				diagnostics.Send(diagnostics.SyncStagesList{Stages: stagesIdsList})
				diagnostics.Send(diagnostics.SnapshotDownloadStatistics{
					Downloaded:           stats.BytesCompleted,
					Total:                stats.BytesTotal,
					TotalTime:            time.Since(downloadStartTime).Round(time.Second).Seconds(),
					DownloadRate:         stats.DownloadRate,
					UploadRate:           stats.UploadRate,
					Peers:                stats.PeersUnique,
					Files:                stats.FilesTotal,
					Connections:          stats.ConnectionsTotal,
					Alloc:                m.Alloc,
					Sys:                  m.Sys,
					DownloadFinished:     stats.Completed,
					TorrentMetadataReady: stats.MetadataReady,
				})

				if stats.MetadataReady < stats.FilesTotal {
					log.Info(fmt.Sprintf("[%s] Waiting for torrents metadata: %d/%d", logPrefix, stats.MetadataReady, stats.FilesTotal))
					continue
				}

				dbg.ReadMemStats(&m)
				downloadTimeLeft := calculateTime(stats.BytesTotal-stats.BytesCompleted, stats.DownloadRate)
				suffix := "downloading"
				if stats.Progress > 0 && stats.DownloadRate == 0 {
					suffix += " (or verifying)"
				}

				log.Info(fmt.Sprintf("[%s] %s", logPrefix, suffix),
					"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common.ByteCount(stats.BytesCompleted), common.ByteCount(stats.BytesTotal)),
					"time-left", downloadTimeLeft,
					"total-time", time.Since(downloadStartTime).Round(time.Second).String(),
					"download", common.ByteCount(stats.DownloadRate)+"/s",
					"upload", common.ByteCount(stats.UploadRate)+"/s",
					"peers", stats.PeersUnique,
					"files", stats.FilesTotal,
					"connections", stats.ConnectionsTotal,
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
				)
			}
		}
	}

Finish:
	if blockReader.FreezingCfg().Verify {
		if _, err := snapshotDownloader.Verify(ctx, &proto_downloader.VerifyRequest{}); err != nil {
			return err
		}
	}
	stats, err = snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{})
	if err != nil {
		return err
	}
	if !stats.Completed {
		goto Loop
	}

	if err := snapshots.ReopenFolder(); err != nil {
		return err
	}
	if cc.Bor != nil {
		if err := borSnapshots.ReopenFolder(); err != nil {
			return err
		}
	}
	if err := agg.OpenFolder(); err != nil {
		return err
	}

	// Erigon "download once" - means restart/upgrade/downgrade will not download files (and will be fast)
	// After "download once" - Erigon will produce and seed new files
	// Downloader will able: seed new files (already existing on FS), download uncomplete parts of existing files (if Verify found some bad parts)
	if _, err := snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{}); err != nil {
		return err
	}

	if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), agg.Files()); err != nil {
		return err
	}

	firstNonGenesis, err := rawdbv3.SecondKey(tx, kv.Headers)
	if err != nil {
		return err
	}
	if firstNonGenesis != nil {
		firstNonGenesisBlockNumber := binary.BigEndian.Uint64(firstNonGenesis)
		if snapshots.SegmentsMax()+1 < firstNonGenesisBlockNumber {
			log.Warn(fmt.Sprintf("[%s] Some blocks are not in snapshots and not in db", logPrefix), "max_in_snapshots", snapshots.SegmentsMax(), "min_in_db", firstNonGenesisBlockNumber)
		}
	}
	return nil
}

func calculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}
