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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	downloaderproto "github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

func preverifiedNames(names ...string) snapcfg.PreverifiedItems {
	items := make(snapcfg.PreverifiedItems, 0, len(names))
	for _, n := range names {
		items = append(items, snapcfg.PreverifiedItem{Name: n, Hash: "00"})
	}
	return items
}

// A mode-C/D unwind rebuilds the block straddlers covering toBlock, and
// execution restarts from inside that range. Under minimal pruning the
// transactions straddler can be retired while headers and bodies stay,
// so the unwind must be able to name the preverified segment to fetch.
func TestNeededPreverifiedTransactionsForBlock_PicksCoveringSegment(t *testing.T) {
	items := preverifiedNames(
		"v1.1-003400-003500-headers.seg",
		"v1.1-003400-003500-bodies.seg",
		"v2.0-003400-003500-transactions.idx",
		"v2.0-003400-003500-transactions-to-block.idx",
		"v1.1-003300-003400-transactions.seg",
		"v1.1-003400-003500-transactions.seg",
		"v1.1-003500-003600-transactions.seg",
	)
	got, ok := neededPreverifiedTransactionsForBlock(items, 3_499_240)
	require.True(t, ok)
	require.Equal(t, "v1.1-003400-003500-transactions.seg", got.Name)
}

func TestNeededPreverifiedTransactionsForBlock_RangeIsHalfOpen(t *testing.T) {
	items := preverifiedNames(
		"v1.1-003400-003500-transactions.seg",
		"v1.1-003500-003600-transactions.seg",
	)
	got, ok := neededPreverifiedTransactionsForBlock(items, 3_500_000)
	require.True(t, ok)
	require.Equal(t, "v1.1-003500-003600-transactions.seg", got.Name,
		"a segment's To is exclusive, so toBlock == To belongs to the next segment")
}

func TestNeededPreverifiedTransactionsForBlock_PrefersWidest(t *testing.T) {
	items := preverifiedNames(
		"v1.1-003490-003500-transactions.seg",
		"v1.1-003400-003500-transactions.seg",
		"v1.1-003499-003500-transactions.seg",
	)
	got, ok := neededPreverifiedTransactionsForBlock(items, 3_499_240)
	require.True(t, ok)
	require.Equal(t, "v1.1-003400-003500-transactions.seg", got.Name,
		"the rebuild slices from the straddler's From, matching straddleBlockFileForType's widest choice")
}

func TestNeededPreverifiedTransactionsForBlock_NoneCovering(t *testing.T) {
	items := preverifiedNames(
		"v1.1-003400-003500-headers.seg",
		"v1.1-003400-003500-bodies.seg",
		"v2.0-003400-003500-transactions.idx",
		"v1.1-003500-003600-transactions.seg",
	)
	_, ok := neededPreverifiedTransactionsForBlock(items, 3_499_240)
	require.False(t, ok)
}

func addBlockSegToInventory(t *testing.T, p *Provider, name string) {
	t.Helper()
	info, _, ok := snaptype.ParseFileName(p.snapDir, name)
	require.True(t, ok, "parse %s", name)
	require.NoError(t, p.Inventory.AddFile(&snapshot.FileEntry{
		Name: name, FromBlock: info.From, ToBlock: info.To, Local: true, Advertisable: true,
	}))
}

func straddledProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{snapDir: t.TempDir(), Inventory: snapshot.NewInventory()}
	addBlockSegToInventory(t, p, "v1.1-003400-003500-headers.seg")
	addBlockSegToInventory(t, p, "v1.1-003400-003500-bodies.seg")
	return p
}

const straddledTarget = uint64(3_499_240)

func TestPlanTransactionsStraddle_DownloadsPrunedStraddler(t *testing.T) {
	p := straddledProvider(t)
	items := preverifiedNames("v1.1-003400-003500-transactions.seg")
	action, item, err := p.planTransactionsStraddle(items, straddledTarget)
	require.NoError(t, err)
	require.Equal(t, txStraddleDownload, action)
	require.Equal(t, "v1.1-003400-003500-transactions.seg", item.Name)
}

func TestPlanTransactionsStraddle_RegistersSegmentAlreadyOnDisk(t *testing.T) {
	p := straddledProvider(t)
	name := "v1.1-003400-003500-transactions.seg"
	require.NoError(t, os.WriteFile(filepath.Join(p.snapDir, name), []byte("seg"), 0o600))
	action, item, err := p.planTransactionsStraddle(preverifiedNames(name), straddledTarget)
	require.NoError(t, err)
	require.Equal(t, txStraddleRegisterLocal, action)
	require.Equal(t, name, item.Name)
}

func TestPlanTransactionsStraddle_NoopWhenTransactionsStraddlerPresent(t *testing.T) {
	p := straddledProvider(t)
	addBlockSegToInventory(t, p, "v1.1-003400-003500-transactions.seg")
	action, _, err := p.planTransactionsStraddle(preverifiedNames("v1.1-003400-003500-transactions.seg"), straddledTarget)
	require.NoError(t, err)
	require.Equal(t, txStraddleNoop, action)
}

func TestPlanTransactionsStraddle_NoopWithoutBlockStraddle(t *testing.T) {
	p := &Provider{snapDir: t.TempDir(), Inventory: snapshot.NewInventory()}
	action, _, err := p.planTransactionsStraddle(preverifiedNames("v1.1-003400-003500-transactions.seg"), straddledTarget)
	require.NoError(t, err)
	require.Equal(t, txStraddleNoop, action)
}

// Committing an unwind whose restart range has no transactions leaves a
// node that can never execute forward, so the unwind must refuse instead.
func TestPlanTransactionsStraddle_RefusesWhenNothingCoversTarget(t *testing.T) {
	p := straddledProvider(t)
	_, _, err := p.planTransactionsStraddle(preverifiedNames("v1.1-003500-003600-transactions.seg"), straddledTarget)
	require.Error(t, err)
	require.ErrorContains(t, err, "transactions")
}

// materialisingDownloaderClient writes every requested file into dir, the
// way a completed download would, and records the requested names.
type materialisingDownloaderClient struct {
	recordingDownloaderClient
	dir       string
	requested []string
	skipWrite bool
}

func (c *materialisingDownloaderClient) Download(_ context.Context, req *downloaderproto.DownloadRequest) error {
	for _, it := range req.Items {
		c.requested = append(c.requested, it.Path)
		if !c.skipWrite {
			if err := os.WriteFile(filepath.Join(c.dir, it.Path), []byte("seg"), 0o600); err != nil {
				return err
			}
		}
	}
	return nil
}

func requireTransactionsStraddlerRegistered(t *testing.T, p *Provider) {
	t.Helper()
	fi, err := p.straddleBlockFileForType(straddledTarget, snaptype2.Enums.Transactions)
	require.NoError(t, err)
	require.NotNil(t, fi, "transactions straddler must be in Inventory so the rebuild slices it")
	require.Equal(t, "v1.1-003400-003500-transactions.seg", fi.Name())
}

func TestEnsureTransactionsStraddle_DownloadsAndRegisters(t *testing.T) {
	p := straddledProvider(t)
	dl := &materialisingDownloaderClient{dir: p.snapDir}
	p.downloaderClient = dl
	require.NoError(t, p.ensureTransactionsStraddle(t.Context(), preverifiedNames("v1.1-003400-003500-transactions.seg"), straddledTarget))
	require.Equal(t, []string{"v1.1-003400-003500-transactions.seg"}, dl.requested)
	requireTransactionsStraddlerRegistered(t, p)
}

func TestEnsureTransactionsStraddle_RegistersLocalWithoutDownload(t *testing.T) {
	p := straddledProvider(t)
	name := "v1.1-003400-003500-transactions.seg"
	require.NoError(t, os.WriteFile(filepath.Join(p.snapDir, name), []byte("seg"), 0o600))
	require.NoError(t, p.ensureTransactionsStraddle(t.Context(), preverifiedNames(name), straddledTarget))
	requireTransactionsStraddlerRegistered(t, p)
}

func TestEnsureTransactionsStraddle_RefusesWhenDownloadNeededWithoutDownloader(t *testing.T) {
	p := straddledProvider(t)
	err := p.ensureTransactionsStraddle(t.Context(), preverifiedNames("v1.1-003400-003500-transactions.seg"), straddledTarget)
	require.Error(t, err)
	require.ErrorContains(t, err, "downloader")
}

func TestEnsureTransactionsStraddle_RefusesWhenDownloadLeavesNoFile(t *testing.T) {
	p := straddledProvider(t)
	p.downloaderClient = &materialisingDownloaderClient{dir: p.snapDir, skipWrite: true}
	err := p.ensureTransactionsStraddle(t.Context(), preverifiedNames("v1.1-003400-003500-transactions.seg"), straddledTarget)
	require.Error(t, err)
}

func TestEnsureTransactionsStraddle_PropagatesRefusal(t *testing.T) {
	p := straddledProvider(t)
	p.downloaderClient = &materialisingDownloaderClient{dir: p.snapDir}
	err := p.ensureTransactionsStraddle(t.Context(), preverifiedNames("v1.1-003500-003600-transactions.seg"), straddledTarget)
	require.Error(t, err)
	require.ErrorContains(t, err, "transactions")
}
