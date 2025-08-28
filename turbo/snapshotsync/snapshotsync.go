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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/downloader/downloadergrpc"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
)

var GreatOtterBanner = `
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

type DownloadRequest struct {
	Path        string
	TorrentHash string
}

func NewDownloadRequest(path string, torrentHash string) DownloadRequest {
	return DownloadRequest{Path: path, TorrentHash: torrentHash}
}

func BuildProtoRequest(downloadRequest []DownloadRequest) *proto_downloader.AddRequest {
	req := &proto_downloader.AddRequest{Items: make([]*proto_downloader.AddItem, 0, len(snaptype2.BlockSnapshotTypes))}
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
func RequestSnapshotsDownload(
	ctx context.Context,
	downloadRequest []DownloadRequest,
	downloader proto_downloader.DownloaderClient,
	logPrefix string,
) error {
	preq := &proto_downloader.SetLogPrefixRequest{Prefix: logPrefix}
	downloader.SetLogPrefix(ctx, preq)
	// start seed large .seg of large size
	req := BuildProtoRequest(downloadRequest)
	if _, err := downloader.Add(ctx, req, grpc.WaitForReady(true)); err != nil {
		return err
	}
	return nil
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

func isStateSnapshot(name string) bool {
	return isStateHistory(name) || strings.HasPrefix(name, "domain")
}
func isStateHistory(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history") || strings.HasPrefix(name, "accessor")
}
func canSnapshotBePruned(name string) bool {
	return (isStateHistory(name) || strings.Contains(name, "transactions")) && !strings.Contains(name, "rcache")
}

func buildBlackListForPruning(
	pruneMode bool,
	stepPrune, minBlockToDownload, blockPrune uint64,
	preverified snapcfg.Preverified,
) (map[string]struct{}, error) {

	blackList := make(map[string]struct{})
	if !pruneMode {
		return blackList, nil
	}
	blockPrune = adjustBlockPrune(blockPrune, minBlockToDownload)
	for _, p := range preverified.Items {
		name := p.Name
		// Don't prune unprunable files
		if !canSnapshotBePruned(name) {
			continue
		}
		if isStateSnapshot(name) {
			// parse "from" (0) and "to" (64) from the name
			// parse the snapshot "kind". e.g kind of 'idx/v1.0-accounts.0-64.ef' is "idx/v1.0-accounts"
			res, _, ok := snaptype.ParseFileName("", name)
			if !ok {
				return blackList, errors.New("invalid state snapshot name")
			}
			if stepPrune < res.To {
				continue
			}
			blackList[name] = struct{}{}
		} else {
			// e.g 'v1.0-000000-000100-beaconblocks.seg'
			// parse "from" (000000) and "to" (000100) from the name. 100 is 100'000 blocks
			res, _, ok := snaptype.ParseFileName("", name)
			if !ok {
				continue
			}
			if blockPrune < res.To {
				continue
			}
			blackList[name] = struct{}{}
		}
	}

	return blackList, nil
}

type blockReader interface {
	Snapshots() BlockSnapshots
	BorSnapshots() BlockSnapshots
	IterateFrozenBodies(_ func(blockNum uint64, baseTxNum uint64, txCount uint64) error) error
	FreezingCfg() ethconfig.BlocksFreezing
	AllTypes() []snaptype.Type
	FrozenFiles() (list []string)
	TxnumReader(ctx context.Context) rawdbv3.TxNumsReader
}

// getMinimumBlocksToDownload - get the minimum number of blocks to download
func getMinimumBlocksToDownload(
	ctx context.Context,
	blockReader blockReader,
	maxStateStep uint64,
	historyPruneTo uint64,
) (minBlockToDownload uint64, minStateStepToDownload uint64, err error) {
	started := time.Now()
	var iterations int64
	defer func() {
		log.Debug("getMinimumBlocksToDownload finished",
			"timeTaken", time.Since(started),
			"iterations", iterations,
			"err", err)
	}()
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	minToDownload := uint64(math.MaxUint64)
	minStateStepToDownload = uint64(math.MaxUint32)
	stateTxNum := maxStateStep * config3.DefaultStepSize
	if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
		if iterations%1e6 == 0 {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
		}
		iterations++
		if blockNum == historyPruneTo {
			minStateStepToDownload = (baseTxNum - (config3.DefaultStepSize - 1)) / config3.DefaultStepSize
			if baseTxNum < (config3.DefaultStepSize - 1) {
				minStateStepToDownload = 0
			}
		}
		if stateTxNum <= baseTxNum { // only consider the block if it
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
	return frozenBlocks - minToDownload, minStateStepToDownload, nil
}

func getMaxStepRangeInSnapshots(preverified snapcfg.Preverified) (uint64, error) {
	maxTo := uint64(0)
	for _, p := range preverified.Items {
		// take the "to" from "domain" snapshot
		if !strings.HasPrefix(p.Name, "domain") {
			continue
		}
		name := strings.TrimPrefix(p.Name, "domain/")
		versionString := strings.Split(name, "-")[0]
		name = strings.TrimPrefix(name, versionString)

		rangeString := strings.Split(name, ".")[1]
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

func computeBlocksToPrune(blockReader blockReader, p prune.Mode) (blocksToPrune uint64, historyToPrune uint64) {
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	return p.Blocks.PruneTo(frozenBlocks), p.History.PruneTo(frozenBlocks)
}

// isTransactionsSegmentExpired - check if the transactions segment is expired according to whichever history expiry policy we use.
func isTransactionsSegmentExpired(cc *chain.Config, pruneMode prune.Mode, p snapcfg.PreverifiedItem) bool {
	// History expiry is the default.
	if pruneMode.Blocks != prune.DefaultBlocksPruneMode {
		return false
	}

	// We use the pre-merge data policy.
	s, _, ok := snaptype.ParseFileName("", p.Name)
	if !ok {
		return false
	}
	return cc.IsPreMerge(s.From)
}

// isReceiptsSegmentExpired - check if the receipts segment is expired according to whichever history expiry policy we use.
func isReceiptsSegmentPruned(tx kv.RwTx, txNumsReader rawdbv3.TxNumsReader, cc *chain.Config, pruneMode prune.Mode, head uint64, p snapcfg.PreverifiedItem) bool {
	if strings.Contains(p.Name, "domain") {
		return false // domain snapshots are never pruned
	}
	pruneHeight := pruneMode.Blocks.PruneTo(head) // if a receipt is below this height, it is pruned
	if pruneMode.Blocks == prune.DefaultBlocksPruneMode && cc.MergeHeight != nil {
		pruneHeight = cc.MergeHeight.Uint64()
	}

	// We use the pre-merge data policy.
	s, _, ok := snaptype.ParseFileName("", p.Name)
	if !ok {
		return false
	}
	minTxNum, err := txNumsReader.Min(tx, pruneHeight)
	if err != nil {
		log.Crit("Failed to get minimum transaction number", "err", err)
		return false
	}
	minStep := minTxNum / config3.DefaultStepSize
	return s.From < minStep
}

// unblackListFilesBySubstring - removes files from the blacklist that match any of the provided substrings.
func unblackListFilesBySubstring(blackList map[string]struct{}, strs ...string) {
	for _, str := range strs {
		for k := range blackList {
			if strings.Contains(k, str) {
				delete(blackList, k)
			}
		}
	}
}

// SyncSnapshots - Check snapshot states, determine what needs to be requested from the downloader
// then wait for downloads to complete.
func SyncSnapshots(
	ctx context.Context,
	logPrefix, task string,
	headerchain, blobs, caplinState bool,
	prune prune.Mode,
	caplin CaplinMode,
	tx kv.RwTx,
	blockReader blockReader,
	cc *chain.Config,
	snapshotDownloader proto_downloader.DownloaderClient,
	syncCfg ethconfig.Sync,
) error {
	if blockReader.FreezingCfg().NoDownloader || snapshotDownloader == nil {
		return nil
	}
	snapCfg, _ := snapcfg.KnownCfg(cc.ChainName)
	// Skip getMinimumBlocksToDownload if we can because it's slow.
	if snapCfg.Local {
		// This belongs higher up the call chain.
		if !headerchain {
			log.Info(fmt.Sprintf("[%s] Skipping SyncSnapshots, local preverified. Use snapshots reset to resync", logPrefix))
		}
	} else {
		txNumsReader := blockReader.TxnumReader(ctx)

		// This clause belongs in another function.
		log.Info(fmt.Sprintf("[%s] Checking %s", logPrefix, task))

		frozenBlocks := blockReader.Snapshots().SegmentsMax()
		//Corner cases:
		// - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
		// - Erigon "download once": means restart/upgrade/downgrade must not download files (and will be fast)
		// - After "download once" - Erigon will produce and seed new files

		// send all hashes to the Downloader service
		preverifiedBlockSnapshots := snapCfg.Preverified
		downloadRequest := make([]DownloadRequest, 0, len(preverifiedBlockSnapshots.Items))

		blockPrune, historyPrune := computeBlocksToPrune(blockReader, prune)
		blackListForPruning := make(map[string]struct{})
		wantToPrune := prune.Blocks.Enabled() || prune.History.Enabled()
		if !headerchain && wantToPrune {
			maxStateStep, err := getMaxStepRangeInSnapshots(preverifiedBlockSnapshots)
			if err != nil {
				return err
			}
			minBlockToDownload, minStepToDownload, err := getMinimumBlocksToDownload(ctx, blockReader, maxStateStep, historyPrune)
			if err != nil {
				return err
			}

			blackListForPruning, err = buildBlackListForPruning(wantToPrune, minStepToDownload, minBlockToDownload, blockPrune, preverifiedBlockSnapshots)
			if err != nil {
				return err
			}
		}

		// If we want to get all receipts, we also need to unblack list log indexes (otherwise eth_getLogs won't work).
		if syncCfg.PersistReceiptsCacheV2 {
			unblackListFilesBySubstring(blackListForPruning, kv.LogAddrIdx.String(), kv.LogTopicIdx.String())
		}

		// build all download requests
		for _, p := range preverifiedBlockSnapshots.Items {
			if caplin == NoCaplin && (strings.Contains(p.Name, "beaconblocks") || strings.Contains(p.Name, "blobsidecars") || strings.Contains(p.Name, "caplin")) {
				continue
			}
			if caplin == OnlyCaplin && !strings.Contains(p.Name, "beaconblocks") && !strings.Contains(p.Name, "blobsidecars") && !strings.Contains(p.Name, "caplin") {
				continue
			}

			if isStateSnapshot(p.Name) && blockReader.FreezingCfg().DisableDownloadE3 {
				continue
			}
			if !blobs && strings.Contains(p.Name, snaptype.BlobSidecars.Name()) {
				continue
			}
			if !caplinState && strings.Contains(p.Name, "caplin/") {
				continue
			}
			if headerchain &&
				!(strings.Contains(p.Name, "headers") || strings.Contains(p.Name, "bodies") || p.Name == "salt-blocks.txt") {
				continue
			}
			if !syncCfg.KeepExecutionProofs && isStateHistory(p.Name) && strings.Contains(p.Name, kv.CommitmentDomain.String()) {
				continue
			}

			if !syncCfg.PersistReceiptsCacheV2 && isStateSnapshot(p.Name) && strings.Contains(p.Name, kv.RCacheDomain.String()) {
				continue
			}

			if strings.Contains(p.Name, "transactions") && isTransactionsSegmentExpired(cc, prune, p) {
				continue
			}

			isRcacheRelatedSegment := strings.Contains(p.Name, kv.RCacheDomain.String()) ||
				strings.Contains(p.Name, kv.LogAddrIdx.String()) ||
				strings.Contains(p.Name, kv.LogTopicIdx.String())

			if isRcacheRelatedSegment && isReceiptsSegmentPruned(tx, txNumsReader, cc, prune, frozenBlocks, p) {
				continue
			}

			if _, ok := blackListForPruning[p.Name]; ok {
				continue
			}

			downloadRequest = append(downloadRequest, DownloadRequest{
				Path:        p.Name,
				TorrentHash: p.Hash,
			})
		}

		// Only add the preverified hashes until the initial sync completed for the first time.

		log.Info(fmt.Sprintf("[%s] Requesting %s from downloader", logPrefix, task))
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := RequestSnapshotsDownload(ctx, downloadRequest, snapshotDownloader, logPrefix); err != nil {
				log.Error(fmt.Sprintf("[%s] call downloader", logPrefix), "err", err)
				time.Sleep(10 * time.Second)
				continue
			}
			break
		}
	}

	// Check for completion immediately, then growing intervals.
	interval := time.Second
	for {
		completedResp, err := snapshotDownloader.Completed(ctx, &proto_downloader.CompletedRequest{})
		if err != nil {
			return fmt.Errorf("waiting for snapshot download: %w", err)
		}
		if completedResp.GetCompleted() {
			break
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(interval):
		}
		interval = min(interval*2, 20*time.Second)
	}
	log.Info(fmt.Sprintf("[%s] Downloader completed %s", logPrefix, task))
	log.Info(fmt.Sprintf("[%s] Synced %s", logPrefix, task))
	return nil
}
