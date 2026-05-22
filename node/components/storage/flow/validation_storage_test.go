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
