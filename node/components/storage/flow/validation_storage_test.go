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

package flow

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// rejectingValidator is the test sentinel — accepts only files whose
// Name matches `accept`, otherwise returns a fixed error.
type rejectingValidator struct{ accept string }

func (rejectingValidator) Name() string { return "test_reject" }
func (r rejectingValidator) Validate(file *snapshot.FileEntry, _ validation.ContentSource) error {
	if file == nil || file.Name != r.accept {
		return errors.New("test rejection")
	}
	return nil
}

func TestInventoryStorage_NoChainRecordsEverything(t *testing.T) {
	inv := snapshot.NewInventory()
	storage := NewInventoryStorage(inv, nil, "")

	require.NoError(t, storage.RecordFile(&snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.kv", Domain: testDomain,
		FromStep: 0, ToStep: 1024, Local: true,
	}))
	require.Len(t, inv.AllDomainFiles(testDomain), 1)
}

func TestInventoryStorage_ChainAdmitsValidFile(t *testing.T) {
	inv := snapshot.NewInventory()
	chain := validation.Chain{rejectingValidator{accept: "ok.kv"}}
	storage := NewInventoryStorage(inv, chain, "")

	require.NoError(t, storage.RecordFile(&snapshot.FileEntry{
		Name: "ok.kv", Domain: testDomain,
		FromStep: 0, ToStep: 1024, Local: true,
	}))
	require.Len(t, inv.AllDomainFiles(testDomain), 1)
}

func TestInventoryStorage_ChainRejectsInvalidFileNotInInventory(t *testing.T) {
	inv := snapshot.NewInventory()
	chain := validation.Chain{rejectingValidator{accept: "ok.kv"}}
	storage := NewInventoryStorage(inv, chain, "")

	err := storage.RecordFile(&snapshot.FileEntry{
		Name: "bad.kv", Domain: testDomain,
		FromStep: 0, ToStep: 1024, Local: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "validation: test_reject: test rejection")
	require.Empty(t, inv.AllDomainFiles(testDomain),
		"validation rejection must keep the file out of the inventory")
}

func TestInventoryStorage_DefaultStage1ChainRejectsEmptyName(t *testing.T) {
	inv := snapshot.NewInventory()
	storage := NewInventoryStorage(inv, validation.DefaultStage1Chain(), "")

	err := storage.RecordFile(&snapshot.FileEntry{
		Name: "", FromStep: 0, ToStep: 1024,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "name_not_empty")
}

func TestInventoryStorage_DefaultStage1ChainRejectsInvertedRange(t *testing.T) {
	inv := snapshot.NewInventory()
	storage := NewInventoryStorage(inv, validation.DefaultStage1Chain(), "")

	err := storage.RecordFile(&snapshot.FileEntry{
		Name: "x.kv", FromStep: 2048, ToStep: 1024,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "range_ordering")
}

// TestInventoryStorage_ResolvesFileInKindSubdir is the gap-I regression
// guard. Inventory entry names are bare basenames, but the downloaded
// file lives in its kind's subdir (domain/, history/, idx/, …) — the
// publisher seeds via RelPathForName so the torrent info.Name carries
// the subdir-prefixed form, and the downloader writes there. RecordFile's
// content-presence check must resolve the file at PathForName(snapDir,
// name); joining the bare name looks at the top level, ContentNotEmpty
// reports "marked Local but not present on disk", RecordFile fails,
// statePending never drains, and InitialStateReady never fires.
func TestInventoryStorage_ResolvesFileInKindSubdir(t *testing.T) {
	snapDir := t.TempDir()
	mustWrite := func(rel string, b []byte) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Join(snapDir, filepath.Dir(rel)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, rel), b, 0o644))
	}
	// Files on disk in their kind subdirs, the way the downloader wrote
	// them (torrent info.Name = RelPathForName(name)).
	mustWrite("domain/v1.0-accounts.0-1024.kv", []byte("state"))
	mustWrite("history/v1.0-accounts.0-1024.v", []byte("hist"))
	mustWrite("idx/v1.0-accounts.0-1024.ef", []byte("idx"))
	// Sanity: none of them are at the top level — a bare-name lookup fails.
	for _, bare := range []string{"v1.0-accounts.0-1024.kv", "v1.0-accounts.0-1024.v", "v1.0-accounts.0-1024.ef"} {
		_, err := os.Stat(filepath.Join(snapDir, bare))
		require.True(t, os.IsNotExist(err), "%s should not exist at top level", bare)
	}

	inv := snapshot.NewInventory()
	storage := NewInventoryStorage(inv, validation.DefaultStage1ChainWithDisk(snapDir), snapDir)

	// Bare entry names — exactly what the orchestrator records on
	// DownloadComplete. RecordFile must find each in its subdir.
	require.NoError(t, storage.RecordFile(&snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.kv", Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV,
		FromStep: 0, ToStep: 1024, Local: true, Trust: snapshot.TrustVerified,
	}))
	require.NoError(t, storage.RecordFile(&snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.v", Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory,
		FromStep: 0, ToStep: 1024, Local: true, Trust: snapshot.TrustVerified,
	}))
	require.NoError(t, storage.RecordFile(&snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.ef", Domain: snapshot.DomainAccounts, Kind: snapshot.KindIdx,
		FromStep: 0, ToStep: 1024, Local: true, Trust: snapshot.TrustVerified,
	}))
	require.Len(t, inv.AllDomainFiles(snapshot.DomainAccounts), 3)
}
