package snapshotsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"github.com/ledgerwatch/log/v3"
)

func BuildProtoRequest(downloadRequest []services.DownloadRequest) *proto_downloader.DownloadRequest {
	req := &proto_downloader.DownloadRequest{Items: make([]*proto_downloader.DownloadItem, 0, len(snaptype.AllSnapshotTypes))}
	for _, r := range downloadRequest {
		if r.Path != "" {
			if r.TorrentHash != "" {
				req.Items = append(req.Items, &proto_downloader.DownloadItem{
					TorrentHash: downloadergrpc.String2Proto(r.TorrentHash),
					Path:        r.Path,
				})
			} else {
				req.Items = append(req.Items, &proto_downloader.DownloadItem{
					Path: r.Path,
				})
			}
		} else {
			if r.Ranges.To-r.Ranges.From != snaptype.Erigon2SegmentSize {
				continue
			}
			if r.Bor {
				for _, t := range []snaptype.Type{snaptype.BorEvents, snaptype.BorSpans} {
					req.Items = append(req.Items, &proto_downloader.DownloadItem{
						Path: snaptype.SegmentFileName(r.Ranges.From, r.Ranges.To, t),
					})
				}
			} else {
				for _, t := range snaptype.AllSnapshotTypes {
					req.Items = append(req.Items, &proto_downloader.DownloadItem{
						Path: snaptype.SegmentFileName(r.Ranges.From, r.Ranges.To, t),
					})
				}
			}
		}
	}
	return req
}

// RequestSnapshotsDownload - builds the snapshots download request and downloads them
func RequestSnapshotsDownload(ctx context.Context, downloadRequest []services.DownloadRequest, downloader proto_downloader.DownloaderClient) error {
	// start seed large .seg of large size
	req := BuildProtoRequest(downloadRequest)
	if _, err := downloader.Download(ctx, req); err != nil {
		return err
	}
	return nil
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(logPrefix string, ctx context.Context, histV3 bool, agg *state.AggregatorV3, tx kv.RwTx, blockReader services.FullBlockReader, notifier services.DBEventNotifier, cc *chain.Config, snapshotDownloader proto_downloader.DownloaderClient) error {
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
		if notifier != nil { // can notify right here, even that write txn is not commit
			notifier.OnNewSnapshot()
		}
		return nil
	}

	// Original intent of snInDB was to contain the file names of the snapshot files for the very first run of the Erigon instance
	// Then, we would insist to only download such files, and no others (whitelist)
	// However, at some point later, the code was incorrectly changed to update this record in each iteration of the stage loop (function WriteSnapshots)
	// And so this list cannot be relied upon as the whitelist, because it also includes all the files created by the node itself
	// Not sure what to do it is so far, but the temporary solution is to instead use it as a blacklist (existingFilesMap)
	snInDB, snHistInDB, err := rawdb.ReadSnapshots(tx)
	if err != nil {
		return err
	}
	dbEmpty := len(snInDB) == 0
	var existingFilesMap, borExistingFilesMap map[string]struct{}
	var missingSnapshots, borMissingSnapshots []*services.Range
	if !dbEmpty {
		existingFilesMap, missingSnapshots, err = snapshots.ScanDir()
		if err != nil {
			return err
		}
		if cc.Bor == nil {
			borExistingFilesMap = map[string]struct{}{}
		} else {
			borExistingFilesMap, borMissingSnapshots, err = borSnapshots.ScanDir()
			if err != nil {
				return err
			}
		}
	}
	if len(missingSnapshots) > 0 {
		log.Warn(fmt.Sprintf("[%s] downloading missing snapshots", logPrefix))
	}

	// send all hashes to the Downloader service
	preverifiedBlockSnapshots := snapcfg.KnownCfg(cc.ChainName, []string{} /* whitelist */, snHistInDB).Preverified
	downloadRequest := make([]services.DownloadRequest, 0, len(preverifiedBlockSnapshots)+len(missingSnapshots))
	// build all download requests
	// builds preverified snapshots request
	for _, p := range preverifiedBlockSnapshots {
		if !histV3 {
			if strings.HasPrefix(p.Name, "domain") || strings.HasPrefix(p.Name, "history") || strings.HasPrefix(p.Name, "idx") {
				continue
			}
		}

		_, exists := existingFilesMap[p.Name]
		_, borExists := borExistingFilesMap[p.Name]
		if !exists && !borExists { // Not to download existing files "behind the scenes"
			downloadRequest = append(downloadRequest, services.NewDownloadRequest(nil, p.Name, p.Hash, false /* Bor */))
		}
	}

	// builds missing snapshots request
	for _, r := range missingSnapshots {
		downloadRequest = append(downloadRequest, services.NewDownloadRequest(r, "", "", false /* Bor */))
	}
	if cc.Bor != nil {
		for _, r := range borMissingSnapshots {
			downloadRequest = append(downloadRequest, services.NewDownloadRequest(r, "", "", true /* Bor */))
		}
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
				/*
					if !blockReader.FreezingCfg().Verify { // will verify after loop
						if _, err := snapshotDownloader.Verify(ctx, &proto_downloader.VerifyRequest{}); err != nil {
							return err
						}
					}
				*/
				log.Info(fmt.Sprintf("[%s] download finished", logPrefix), "time", time.Since(downloadStartTime).String())
				break Loop
			} else {
				if stats.MetadataReady < stats.FilesTotal {
					log.Info(fmt.Sprintf("[%s] Waiting for torrents metadata: %d/%d", logPrefix, stats.MetadataReady, stats.FilesTotal))
					continue
				}
				dbg.ReadMemStats(&m)
				downloadTimeLeft := calculateTime(stats.BytesTotal-stats.BytesCompleted, stats.DownloadRate)
				suffix := "downloading archives"
				if stats.Progress > 0 && stats.DownloadRate == 0 {
					suffix += "verifying archives"
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

	if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), agg.Files()); err != nil {
		return err
	}
	if notifier != nil { // can notify right here, even that write txn is not commit
		notifier.OnNewSnapshot()
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
