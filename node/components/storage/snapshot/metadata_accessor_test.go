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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInferKind_Accessors pins that every accessor extension classifies
// as KindAccessor — never as a primary kind. Before this, .bt/.kvi/etc.
// were unrecognised and a domain accessor was silently mistaken for a
// domain primary (KindKV).
func TestInferKind_Accessors(t *testing.T) {
	for _, name := range []string{
		"v1.0-accounts.0-128.bt",
		"v1.0-accounts.0-128.kvi",
		"v1.0-accounts.0-128.kvei",
		"v1.0-accounts.0-128.vi",
		"v1.0-accounts.0-128.efi",
		"v1.0-000000-000500-headers.idx",
	} {
		k, ok := InferKind(name)
		require.True(t, ok, "InferKind(%q) should classify", name)
		require.Equal(t, KindAccessor, k, "InferKind(%q)", name)
	}
}

// TestAddFile_DomainAccessorClassification verifies a minimal accessor
// entry (Name only — as the OnFilesChange wire adds it) is auto-filled
// to KindAccessor and bucketed under its domain, not misfiled as a
// domain primary or a block file.
func TestAddFile_DomainAccessorClassification(t *testing.T) {
	inv := NewInventory()
	require.NoError(t, inv.AddFile(&FileEntry{Name: "v1.0-accounts.0-128.bt", Local: true}))

	files := inv.AllDomainFiles(DomainAccounts)
	require.Len(t, files, 1)
	require.Equal(t, KindAccessor, files[0].Kind)
	require.Equal(t, DomainAccounts, files[0].Domain)
	require.Empty(t, inv.BlockFiles(), "domain accessor must not land in the block bucket")
}

// TestAddFile_BlockAccessorClassification verifies a block index (.idx)
// is classified KindAccessor and bucketed with block files.
func TestAddFile_BlockAccessorClassification(t *testing.T) {
	inv := NewInventory()
	require.NoError(t, inv.AddFile(&FileEntry{Name: "v1.0-000000-000500-headers.idx", Local: true}))

	blocks := inv.BlockFiles()
	require.Len(t, blocks, 1)
	require.Equal(t, KindAccessor, blocks[0].Kind)
}
