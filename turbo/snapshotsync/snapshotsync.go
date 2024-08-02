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

package snapshotsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/downloader/downloadergrpc"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"

	"github.com/erigontech/erigon/core/rawdb"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	snaptype2 "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/turbo/services"
)

var greatOtterBanner = `
   _____ _             _   _                ____  _   _                                       
  / ____| |           | | (_)              / __ \| | | |                                      
 | (___ | |_ __ _ _ __| |_ _ _ __   __ _  | |  | | |_| |_ ___ _ __ ___ _   _ _ __   ___       
  \___ \| __/ _ | '__| __| | '_ \ / _ | | |  | | __| __/ _ \ '__/ __| | | | '_ \ / __|      
  ____) | || (_| | |  | |_| | | | | (_| | | |__| | |_| ||  __/ |  \__ \ |_| | | | | (__ _ _ _ 
 |_____/ \__\__,_|_|   \__|_|_| |_|\__, |  \____/ \__|\__\___|_|  |___/\__, |_| |_|\___(_|_|_)
                                    __/ |                               __/ |                 
                                   |___/                               |___/                                            


                                        .:-===++**++===-:                                 
                                   :=##%@@@@@@@@@@@@@@@@@@%#*=.                           
                               .=#@@@@@@%##+====--====+##@@@@@@@#=.     ...               
                   .=**###*=:+#@@@@%*=:.                  .:=#%@@@@#==#@@@@@%#-           
                 -#@@@@%%@@@@@@%+-.                            .=*%@@@@#*+*#@@@%=         
                =@@@*:    -%%+:                                    -#@+.     =@@@-        
                %@@#     +@#.                                        :%%-     %@@*        
                @@@+    +%=.     -+=                        :=-       .#@-    %@@#        
                *@@%:  #@-      =@@@*                      +@@@%.       =@= -*@@@:        
                 #@@@##@+       #@@@@.                     %@@@@=        #@%@@@#-         
                  :#@@@@:       +@@@#       :=++++==-.     *@@@@:        =@@@@-           
                  =%@@%=         +#*.    =#%#+==-==+#%%=:  .+#*:         .#@@@#.          
                 +@@%+.               .+%+-.          :=##-                :#@@@-         
                -@@@=                -%#:     ..::.      +@*                 +@@%.        
    .::-========*@@@..              -@#      +%@@@@%.     -@#               .-@@@+=======-
.:-====----:::::#@@%:--=::::..      #@:      *@@@@@%:      *@=      ..:-:-=--:@@@+::::----
                =@@@:.......        @@        :+@#=.       -@+        .......-@@@:        
       .:=++####*%@@%=--::::..      @@   %#     %*    :@*  -@+      ...::---+@@@#*#*##+=-:
  ..--==::..     :%@@@-   ..:::..   @@   +@*:.-#@@+-.-#@-  -@+   ..:::..  .+@@@#.     ..:-
                  .#@@@##-:.        @@    :+#@%=.:+@@#=.   -@+        .-=#@@@@+           
             -=+++=--+%@@%+=.       @@       +%*=+#%-      -@+       :=#@@@%+--++++=:     
         .=**=:.      .=*@@@@@#=:.  @@         :--.        -@+  .-+#@@@@%+:       .:=*+-. 
        ::.              .=*@@@@@@%#@@+=-:..         ..::=+#@%#@@@@@@%+-.             ..-.
                            ..=*#@@@@@@@@@@@@@@@%%@@@@@@@@@@@@@@%#+-.                     
                                  .:-==++*#######%######**+==-:                           

             
`

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

func adjustStepPrune(steps uint64) uint64 {
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

func adjustBlockPrune(blocks, minBlocksToDownload uint64) uint64 {
	if minBlocksToDownload < snaptype.Erigon2MergeLimit {
		minBlocksToDownload = snaptype.Erigon2MergeLimit
	}
	if blocks > minBlocksToDownload {
		blocks = minBlocksToDownload
	}
	return blocks - blocks%snaptype.Erigon2MergeLimit
}

func shouldUseStepsForPruning(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history")
}

func canSnapshotBePruned(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history") || strings.Contains(name, "transactions")
}

func buildBlackListForPruning(pruneMode bool, stepPrune, minBlockToDownload, blockPrune uint64, preverified snapcfg.Preverified) (map[string]struct{}, error) {

	blackList := make(map[string]struct{})
	if !pruneMode {
		return blackList, nil
	}
	stepPrune = adjustStepPrune(stepPrune)
	blockPrune = adjustBlockPrune(blockPrune, minBlockToDownload)
	for _, p := range preverified {
		name := p.Name
		// Don't prune unprunable files
		if !canSnapshotBePruned(name) {
			continue
		}
		var _, to uint64
		var err error
		if shouldUseStepsForPruning(name) {
			// parse "from" (0) and "to" (64) from the name
			// parse the snapshot "kind". e.g kind of 'idx/v1-accounts.0-64.ef' is "idx/v1-accounts"
			rangeString := strings.Split(name, ".")[1]
			rangeNums := strings.Split(rangeString, "-")
			// convert the range to uint64
			to, err = strconv.ParseUint(rangeNums[1], 10, 64)
			if err != nil {
				return nil, err
			}
			if stepPrune < to {
				continue
			}
			blackList[name] = struct{}{}
		} else {
			// e.g 'v1-000000-000100-beaconblocks.seg'
			// parse "from" (000000) and "to" (000100) from the name. 100 is 100'000 blocks
			s, _, ok := snaptype.ParseFileName("", name)
			if !ok {
				continue
			}
			to = s.To
			if blockPrune < to {
				continue
			}
			blackList[name] = struct{}{}
		}
	}

	return blackList, nil
}

// getMinimumBlocksToDownload - get the minimum number of blocks to download
func getMinimumBlocksToDownload(tx kv.Tx, blockReader services.FullBlockReader, minStep uint64, blockPruneTo, historyPruneTo uint64) (uint64, uint64, error) {
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	minToDownload := uint64(math.MaxUint64)
	minStepToDownload := uint64(math.MaxUint32)
	stateTxNum := minStep * config3.HistoryV3AggregationStep
	if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
		if blockNum == historyPruneTo {
			minStepToDownload = (baseTxNum - (config3.HistoryV3AggregationStep - 1)) / config3.HistoryV3AggregationStep
			if baseTxNum < (config3.HistoryV3AggregationStep - 1) {
				minStepToDownload = 0
			}
		}
		if stateTxNum <= baseTxNum { // only cosnider the block if it
			return nil
		}
		newMinToDownload := uint64(0)
		if frozenBlocks > blockNum {
			newMinToDownload = frozenBlocks - blockNum
		}
		if newMinToDownload < minToDownload {
			minToDownload = newMinToDownload
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}

	// return the minimum number of blocks to download and the minimum step.
	return frozenBlocks - minToDownload, minStepToDownload, nil
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

func computeBlocksToPrune(blockReader services.FullBlockReader, p prune.Mode) (blocksToPrune uint64, historyToPrune uint64) {
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	return p.Blocks.PruneTo(frozenBlocks), p.History.PruneTo(frozenBlocks)
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(ctx context.Context, logPrefix string, dirs datadir.Dirs, headerchain, blobs bool, prune prune.Mode, caplin CaplinMode, agg *state.Aggregator, tx kv.RwTx, blockReader services.FullBlockReader, cc *chain.Config, snapshotDownloader proto_downloader.DownloaderClient, stagesIdsList []string) error {
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

	//Corner cases:
	// - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
	// - Erigon "download once": means restart/upgrade/downgrade must not download files (and will be fast)
	// - After "download once" - Erigon will produce and seed new files

	// send all hashes to the Downloader service
	snapCfg := snapcfg.KnownCfg(cc.ChainName)
	preverifiedBlockSnapshots := snapCfg.Preverified
	downloadRequest := make([]services.DownloadRequest, 0, len(preverifiedBlockSnapshots))

	blockPrune, historyPrune := computeBlocksToPrune(blockReader, prune)
	blackListForPruning := make(map[string]struct{})
	wantToPrune := prune.Blocks.Enabled() || prune.History.Enabled()
	if !headerchain && wantToPrune {
		minStep, err := getMaxStepRangeInSnapshots(preverifiedBlockSnapshots)
		if err != nil {
			return err
		}
		minBlockToDownload, minStepToDownload, err := getMinimumBlocksToDownload(tx, blockReader, minStep, blockPrune, historyPrune)
		if err != nil {
			return err
		}

		blackListForPruning, err = buildBlackListForPruning(wantToPrune, minStepToDownload, minBlockToDownload, blockPrune, preverifiedBlockSnapshots)
		if err != nil {
			return err
		}
	}

	// build all download requests
	for _, p := range preverifiedBlockSnapshots {
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

	// Check once without delay, for faster erigon re-start
	stats, err := snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{})
	if err != nil {
		return err
	}

	// Print download progress until all segments are available

	firstLog := true
	for !stats.Completed {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			if firstLog && headerchain {
				log.Info("[OtterSync] Starting Ottersync")
				log.Info(greatOtterBanner)
				firstLog = false
			}
			if stats, err = snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
				log.Warn("Error while waiting for snapshots progress", "err", err)
			} else {
				logStats(ctx, stats, downloadStartTime, stagesIdsList, logPrefix, headerchain)
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
				logStats(ctx, stats, downloadStartTime, stagesIdsList, logPrefix, headerchain)
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

	if err := agg.OpenFolder(); err != nil {
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
	for _, p := range snaptype2.E3StateTypes {
		snapshotDownloader.ProhibitNewDownloads(ctx, &proto_downloader.ProhibitNewDownloadsRequest{
			Type: p.Name(),
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
			log.Warn(fmt.Sprintf("[%s] Some blocks are not in snapshots and not in db. this could have happend due to the node being stopped at the wrong time, you can fix this with 'rm -rf %s' (this is not equivalent to a full resync)", logPrefix, dirs.Chaindata), "max_in_snapshots", snapshots.SegmentsMax(), "min_in_db", firstNonGenesisBlockNumber)
			return fmt.Errorf("some blocks are not in snapshots and not in db. this could have happend due to the node being stopped at the wrong time, you can fix this with 'rm -rf %s' (this is not equivalent to a full resync)", dirs.Chaindata)
		}
	}
	return nil
}

func logStats(ctx context.Context, stats *proto_downloader.StatsReply, startTime time.Time, stagesIdsList []string, logPrefix string, headerchain bool) {
	var m runtime.MemStats

	logReason := "download"
	if headerchain {
		logReason = "downloading header-chain"
	}
	logEnd := "download finished"
	if headerchain {
		logEnd = "header-chain download finished"
	}

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
		log.Info(fmt.Sprintf("[%s] %s", logPrefix, logEnd), "time", time.Since(startTime).String())
	} else {

		if stats.MetadataReady < stats.FilesTotal && stats.BytesTotal == 0 {
			log.Info(fmt.Sprintf("[%s] Waiting for torrents metadata: %d/%d", logPrefix, stats.MetadataReady, stats.FilesTotal))
		}

		dbg.ReadMemStats(&m)

		var remainingBytes uint64

		if stats.BytesTotal > stats.BytesCompleted {
			remainingBytes = stats.BytesTotal - stats.BytesCompleted
		}

		downloadTimeLeft := calculateTime(remainingBytes, stats.CompletionRate)

		log.Info(fmt.Sprintf("[%s] %s", logPrefix, logReason),
			"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common.ByteCount(stats.BytesCompleted), common.ByteCount(stats.BytesTotal)),
			// TODO: "downloading", stats.Downloading,
			"time-left", downloadTimeLeft,
			"total-time", time.Since(startTime).Round(time.Second).String(),
			"download", common.ByteCount(stats.DownloadRate)+"/s",
			"flush", common.ByteCount(stats.FlushRate)+"/s",
			"hash", common.ByteCount(stats.HashRate)+"/s",
			"complete", common.ByteCount(stats.CompletionRate)+"/s",
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
