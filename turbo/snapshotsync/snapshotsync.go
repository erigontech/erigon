package snapshotsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloaderproto"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
	coresnaptype "github.com/ledgerwatch/erigon/core/snaptype"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type CaplinMode int

const (

	// CaplinModeNone - no caplin mode
	NoCaplin   CaplinMode = 1
	OnlyCaplin CaplinMode = 2
	AlsoCaplin CaplinMode = 3
)

func BuildProtoRequest(downloadRequest []services.DownloadRequest) *proto_downloader.AddRequest {
	req := &proto_downloader.AddRequest{Items: make([]*proto_downloader.AddItem, 0, len(coresnaptype.BlockSnapshotTypes))}
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

func adjustStepPrune(steps uint) uint {
	if steps == 0 {
		return 0
	}
	if steps < snaptype.Erigon3SeedableSteps {
		return snaptype.Erigon3SeedableSteps
	}
	if steps%snaptype.Erigon3SeedableSteps == 0 {
		return steps
	}
	// round to nearest multiple of 64. if less than 64, round to 64
	return steps + steps%snaptype.Erigon3SeedableSteps
}

func adjustBlockPrune(blocks, minBlocksToDownload uint) uint {
	if minBlocksToDownload < snaptype.Erigon2MergeLimit {
		minBlocksToDownload = snaptype.Erigon2MergeLimit
	}
	if blocks < minBlocksToDownload {
		blocks = minBlocksToDownload
	}
	if blocks%snaptype.Erigon2MergeLimit == 0 {
		return blocks
	}
	// round to nearest multiple of 64. if less than 64, round to 64
	return blocks + blocks%snaptype.Erigon2MergeLimit
}

func shouldUseStepsForPruning(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history")
}

func canSnapshotBePruned(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history") || strings.Contains(name, "transactions")
}

func buildBlackListForPruning(pruneMode bool, stepPrune, minBlockToDownload, blockPrune uint, preverified snapcfg.Preverified) (map[string]struct{}, error) {
	fmt.Println("AX", pruneMode, stepPrune, minBlockToDownload, blockPrune)
	type snapshotFileData struct {
		from, to  uint64
		stepBased bool
		name      string
	}
	blackList := make(map[string]struct{})
	if !pruneMode {
		return blackList, nil
	}
	stepPrune = adjustStepPrune(stepPrune)
	blockPrune = adjustBlockPrune(blockPrune, minBlockToDownload)
	snapshotKindToNames := make(map[string][]snapshotFileData)
	for _, p := range preverified {
		name := p.Name
		// Dont prune unprunable files
		if !canSnapshotBePruned(name) {
			continue
		}
		var from, to uint64
		var err error
		var kind string
		if shouldUseStepsForPruning(name) {
			// parse "from" (0) and "to" (64) from the name
			// parse the snapshot "kind". e.g kind of 'idx/v1-accounts.0-64.ef' is "idx/v1-accounts"
			rangeString := strings.Split(name, ".")[1]
			rangeNums := strings.Split(rangeString, "-")
			// convert the range to uint64
			from, err = strconv.ParseUint(rangeNums[0], 10, 64)
			if err != nil {
				return nil, err
			}
			to, err = strconv.ParseUint(rangeNums[1], 10, 64)
			if err != nil {
				return nil, err
			}
			kind = strings.Split(name, ".")[0]
		} else {
			// e.g 'v1-000000-000100-beaconblocks.seg'
			// parse "from" (000000) and "to" (000100) from the name. 100 is 100'000 blocks
			minusSplit := strings.Split(name, "-")
			s, _, ok := snaptype.ParseFileName("", name)
			if !ok {
				continue
			}
			from = s.From
			to = s.To
			kind = minusSplit[3]
		}
		blackList[p.Name] = struct{}{} // Add all of them to the blacklist and remove the ones that are not blacklisted later.
		snapshotKindToNames[kind] = append(snapshotKindToNames[kind], snapshotFileData{
			from:      from,
			to:        to,
			stepBased: shouldUseStepsForPruning(name),
			name:      name,
		})
	}
	// sort the snapshots by "from" and "to" in ascending order
	for _, snapshots := range snapshotKindToNames {
		prunedDistance := uint64(0) // keep track of pruned distance for snapshots
		// sort the snapshots by "from" and "to" in descending order
		sort.Slice(snapshots, func(i, j int) bool {
			if snapshots[i].from == snapshots[j].from {
				return snapshots[i].to > snapshots[j].to
			}
			return snapshots[i].from > snapshots[j].from
		})
		for _, snapshot := range snapshots {
			if snapshot.stepBased {
				if prunedDistance >= uint64(stepPrune) {
					break
				}
			} else if prunedDistance >= uint64(blockPrune) {
				break
			}

			if snapshot.stepBased {
				delete(blackList, snapshot.name)
				prunedDistance += snapshot.to - snapshot.from
				continue
			}
			delete(blackList, snapshot.name)
			prunedDistance += snapshot.to - snapshot.from
		}
	}
	return blackList, nil
}

// getMinimumBlocksToDownload - get the minimum number of blocks to download
func getMinimumBlocksToDownload(tx kv.Tx, blockReader services.FullBlockReader, minStep uint64, expectedPruneBlockAmount uint64) (uint64, uint64, error) {
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	minToDownload := uint64(0)
	minStepToDownload := minStep
	stateTxNum := minStep * config3.HistoryV3AggregationStep
	if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
		if stateTxNum <= baseTxNum { // only cosnider the block if it
			return nil
		}
		if blockNum == frozenBlocks-expectedPruneBlockAmount {
			minStepToDownload = (baseTxNum / config3.HistoryV3AggregationStep) + 1
		}
		newMinToDownload := uint64(0)
		if frozenBlocks > blockNum {
			newMinToDownload = blockNum
		}
		if newMinToDownload > minToDownload {
			minToDownload = newMinToDownload
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}
	minimumStepsToKeep := minStep - minStepToDownload
	if expectedPruneBlockAmount >= frozenBlocks {
		minimumStepsToKeep = 0
	}
	// return the minimum number of blocks to download and the minimum step.
	return minToDownload, minimumStepsToKeep, nil
}

func getMaxStepRangeInSnapshots(preverified snapcfg.Preverified) (uint64, error) {
	maxTo := uint64(0)
	for _, p := range preverified {
		// take the "to" from "domain" snapshot
		if !strings.HasPrefix(p.Name, "domain") {
			continue
		}
		rangeString := strings.Split(p.Name, ".")[1]
		rangeNums := strings.Split(rangeString, "-")
		// convert the range to uint64
		to, err := strconv.ParseUint(rangeNums[1], 10, 64)
		if err != nil {
			return 0, err
		}
		if to > maxTo {
			maxTo = to
		}
	}
	return maxTo, nil
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(ctx context.Context, logPrefix string, headerchain, histV3, blobs, pruneMode bool, blockPrune uint, caplin CaplinMode, agg *state.Aggregator, tx kv.RwTx, blockReader services.FullBlockReader, cc *chain.Config, snapshotDownloader proto_downloader.DownloaderClient, stagesIdsList []string) error {
	snapshots := blockReader.Snapshots()
	borSnapshots := blockReader.BorSnapshots()

	// Find minimum block to download.
	if blockReader.FreezingCfg().NoDownloader || snapshotDownloader == nil {
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

	if headerchain {
		snapshots.Close()
		if cc.Bor != nil {
			borSnapshots.Close()
		}
	}

	//Corner cases:
	// - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
	// - Erigon "download once": means restart/upgrade/downgrade must not download files (and will be fast)
	// - After "download once" - Erigon will produce and seed new files

	// send all hashes to the Downloader service
	snapCfg := snapcfg.KnownCfg(cc.ChainName)
	preverifiedBlockSnapshots := snapCfg.Preverified
	downloadRequest := make([]services.DownloadRequest, 0, len(preverifiedBlockSnapshots))

	blackListForPruning := make(map[string]struct{})
	if !headerchain && pruneMode {
		minStep, err := getMaxStepRangeInSnapshots(preverifiedBlockSnapshots)
		if err != nil {
			return err
		}
		if blockPrune > uint(snapshots.SegmentsMax()) {
			blockPrune = uint(snapshots.SegmentsMax())
		}
		minBlockAmountToDownload, minStepToDownload, err := getMinimumBlocksToDownload(tx, blockReader, minStep, uint64(blockPrune))
		if err != nil {
			return err
		}
		blackListForPruning, err = buildBlackListForPruning(pruneMode, uint(minStepToDownload), uint(minBlockAmountToDownload), blockPrune, preverifiedBlockSnapshots)
		if err != nil {
			return err
		}
	}

	// build all download requests
	for _, p := range preverifiedBlockSnapshots {
		if !histV3 {
			if strings.HasPrefix(p.Name, "domain") || strings.HasPrefix(p.Name, "history") || strings.HasPrefix(p.Name, "idx") {
				continue
			}
		}
		if caplin == NoCaplin && (strings.Contains(p.Name, "beaconblocks") || strings.Contains(p.Name, "blobsidecars")) {
			continue
		}
		if caplin == OnlyCaplin && !strings.Contains(p.Name, "beaconblocks") && !strings.Contains(p.Name, "blobsidecars") {
			continue
		}
		if !blobs && strings.Contains(p.Name, "blobsidecars") {
			continue
		}
		if headerchain && !strings.Contains(p.Name, "headers") && !strings.Contains(p.Name, "bodies") {
			continue
		}
		if _, ok := blackListForPruning[p.Name]; ok {
			continue
		}

		downloadRequest = append(downloadRequest, services.NewDownloadRequest(p.Name, p.Hash))
	}

	log.Info(fmt.Sprintf("[%s] Requesting downloads", logPrefix))
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

	/*diagnostics.RegisterProvider(diagnostics.ProviderFunc(func(ctx context.Context) error {
		return nil
	}), diagnostics.TypeOf(diagnostics.DownloadStatistics{}), log.Root())*/

	// Check once without delay, for faster erigon re-start
	stats, err := snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{})
	if err != nil {
		return err
	}

	// Print download progress until all segments are available

	for !stats.Completed {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			if stats, err = snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
				log.Warn("Error while waiting for snapshots progress", "err", err)
			} else {
				logStats(ctx, stats, downloadStartTime, stagesIdsList, logPrefix, "download")
			}
		}
	}

	if blockReader.FreezingCfg().Verify {
		if _, err := snapshotDownloader.Verify(ctx, &proto_downloader.VerifyRequest{}); err != nil {
			return err
		}

		if stats, err = snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
			log.Warn("Error while waiting for snapshots progress", "err", err)
		}
	}

	for !stats.Completed {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			if stats, err = snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
				log.Warn("Error while waiting for snapshots progress", "err", err)
			} else {
				logStats(ctx, stats, downloadStartTime, stagesIdsList, logPrefix, "download")
			}
		}
	}

	if err := snapshots.ReopenFolder(); err != nil {
		return err
	}

	if cc.Bor != nil {
		if err := borSnapshots.ReopenFolder(); err != nil {
			return err
		}
	}

	if err := agg.OpenFolder(true); err != nil {
		return err
	}

	// ProhibitNewDownloads implies - so only make the download request once,
	//
	// Erigon "download once" - means restart/upgrade/downgrade will not download files (and will be fast)
	// After "download once" - Erigon will produce and seed new files
	// Downloader will able: seed new files (already existing on FS), download uncomplete parts of existing files (if Verify found some bad parts)
	//
	// after the initial call the downloader or snapshot-lock.file will prevent this download from running
	//

	// prohibit new downloads for the files that were downloaded

	// If we only download headers and bodies, we should prohibit only those.
	if headerchain {
		if _, err := snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{
			Type: coresnaptype.Bodies.Name(),
		}); err != nil {
			return err
		}
		if _, err := snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{
			Type: coresnaptype.Headers.Name(),
		}); err != nil {
			return err
		}
		return nil
	}

	// prohibits further downloads, except some exceptions
	for _, p := range blockReader.AllTypes() {
		if _, err := snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{
			Type: p.Name(),
		}); err != nil {
			return err
		}
	}
	for _, p := range snaptype.SeedableV3Extensions() {
		snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{
			Type: p,
		})
	}

	if caplin != NoCaplin {
		for _, p := range snaptype.CaplinSnapshotTypes {
			if p.Enum() == snaptype.BlobSidecars.Enum() && !blobs {
				continue
			}

			if _, err := snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{
				Type: p.Name(),
			}); err != nil {
				return err
			}
		}
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

func logStats(ctx context.Context, stats *proto_downloader.StatsReply, startTime time.Time, stagesIdsList []string, logPrefix string, logReason string) {
	var m runtime.MemStats

	diagnostics.Send(diagnostics.SyncStagesList{Stages: stagesIdsList})
	diagnostics.Send(diagnostics.SnapshotDownloadStatistics{
		Downloaded:           stats.BytesCompleted,
		Total:                stats.BytesTotal,
		TotalTime:            time.Since(startTime).Round(time.Second).Seconds(),
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

	if stats.Completed {
		log.Info(fmt.Sprintf("[%s] download finished", logPrefix), "time", time.Since(startTime).String())
	} else {

		if stats.MetadataReady < stats.FilesTotal && stats.BytesTotal == 0 {
			log.Info(fmt.Sprintf("[%s] Waiting for torrents metadata: %d/%d", logPrefix, stats.MetadataReady, stats.FilesTotal))
		}

		dbg.ReadMemStats(&m)

		var remainingBytes uint64

		if stats.BytesTotal > stats.BytesCompleted {
			remainingBytes = stats.BytesTotal - stats.BytesCompleted
		}

		downloadTimeLeft := calculateTime(remainingBytes, stats.DownloadRate)

		log.Info(fmt.Sprintf("[%s] %s", logPrefix, logReason),
			"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common.ByteCount(stats.BytesCompleted), common.ByteCount(stats.BytesTotal)),
			// TODO: "downloading", stats.Downloading,
			"time-left", downloadTimeLeft,
			"total-time", time.Since(startTime).Round(time.Second).String(),
			"download", common.ByteCount(stats.DownloadRate)+"/s",
			"upload", common.ByteCount(stats.UploadRate)+"/s",
			"peers", stats.PeersUnique,
			"files", stats.FilesTotal,
			"metadata", fmt.Sprintf("%d/%d", stats.MetadataReady, stats.FilesTotal),
			"connections", stats.ConnectionsTotal,
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
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
