// Copyright 2026 The Erigon Authors
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

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// neededPreverifiedTransactionsForBlock returns the preverified transactions
// .seg whose half-open block range contains toBlock, preferring the widest
// so it matches the straddler straddleBlockFileForType would pick locally.
func neededPreverifiedTransactionsForBlock(items snapcfg.PreverifiedItems, toBlock uint64) (snapcfg.PreverifiedItem, bool) {
	var best snapcfg.PreverifiedItem
	var bestSpan uint64
	found := false
	for _, item := range items {
		info, _, ok := snaptype.ParseFileName("", item.Name)
		if !ok || info.Ext != ".seg" || info.Type == nil || info.Type.Enum() != snaptype2.Enums.Transactions {
			continue
		}
		if toBlock < info.From || toBlock >= info.To {
			continue
		}
		if span := info.To - info.From; !found || span > bestSpan {
			best, bestSpan, found = item, span, true
		}
	}
	return best, found
}

type transactionsStraddleAction int

const (
	txStraddleNoop transactionsStraddleAction = iota
	txStraddleRegisterLocal
	txStraddleDownload
)

// planTransactionsStraddle decides how to make the transactions straddler
// for toBlock available, erroring when nothing local or preverified covers it.
func (p *Provider) planTransactionsStraddle(items snapcfg.PreverifiedItems, toBlock uint64) (transactionsStraddleAction, snapcfg.PreverifiedItem, error) {
	if p.Inventory == nil {
		return txStraddleNoop, snapcfg.PreverifiedItem{}, nil
	}
	headers, err := p.straddleBlockFileForType(toBlock, snaptype2.Enums.Headers)
	if err != nil || headers == nil {
		return txStraddleNoop, snapcfg.PreverifiedItem{}, err
	}
	txs, err := p.straddleBlockFileForType(toBlock, snaptype2.Enums.Transactions)
	if err != nil || txs != nil {
		return txStraddleNoop, snapcfg.PreverifiedItem{}, err
	}
	item, ok := neededPreverifiedTransactionsForBlock(items, toBlock)
	if !ok {
		return txStraddleNoop, snapcfg.PreverifiedItem{}, fmt.Errorf(
			"transactions pruned past unwind target: no local or preverified transactions segment covers block %d", toBlock)
	}
	if _, err := os.Stat(filepath.Join(p.snapDir, item.Name)); err == nil {
		return txStraddleRegisterLocal, item, nil
	}
	return txStraddleDownload, item, nil
}

// ensureTransactionsStraddleForUnwind makes the transactions straddler for
// toBlock available before the unwind trims and rebuilds block files. Minimal
// pruning retires transactions below its horizon while headers and bodies
// stay, and execution restarts from inside the straddled range, so the
// segment is fetched from preverified or the unwind is refused.
func (p *Provider) ensureTransactionsStraddleForUnwind(ctx context.Context, toBlock uint64) error {
	if p.Inventory == nil || p.ChainConfig == nil {
		return nil
	}
	var items snapcfg.PreverifiedItems
	if cfg := snapcfg.KnownCfgOrDevnet(p.ChainConfig.ChainName); cfg != nil {
		items = cfg.Preverified.Items
	}
	return p.ensureTransactionsStraddle(ctx, items, toBlock)
}

func (p *Provider) ensureTransactionsStraddle(ctx context.Context, items snapcfg.PreverifiedItems, toBlock uint64) error {
	action, item, err := p.planTransactionsStraddle(items, toBlock)
	if err != nil || action == txStraddleNoop {
		return err
	}
	path := filepath.Join(p.snapDir, item.Name)
	if action == txStraddleDownload {
		if p.downloaderClient == nil {
			return fmt.Errorf("transactions pruned past unwind target: %s covers block %d but no downloader is wired", item.Name, toBlock)
		}
		if p.logger != nil {
			p.logger.Info("[storage] Provider.Unwind: downloading pruned transactions straddler", "toBlock", toBlock, "file", item.Name)
		}
		req := []dbservices.DownloadRequest{{Path: item.Name, TorrentHash: item.Hash}}
		if err := snapshotsync.RequestSnapshotsDownload(ctx, req, p.downloaderClient, "mode-b-transactions-ensure"); err != nil {
			return fmt.Errorf("download transactions straddler %s: %w", item.Name, err)
		}
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("transactions straddler %s not on disk after download: %w", item.Name, err)
		}
	}
	info, _, ok := snaptype.ParseFileName(p.snapDir, item.Name)
	if !ok {
		return fmt.Errorf("parse transactions straddler name %s", item.Name)
	}
	return p.Inventory.AddFile(&snapshot.FileEntry{
		Name:         item.Name,
		FromBlock:    info.From,
		ToBlock:      info.To,
		Local:        true,
		Advertisable: true,
	})
}
