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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestHeaderChainValidator_NonHeaderFilesShortCircuit(t *testing.T) {
	t.Parallel()
	v := HeaderChainValidator{}
	cases := [][]*snapshot.FileEntry{
		nil,
		{},
		{{Domain: snapshot.DomainAccounts, Name: "v1.1-accounts.0-128.kv"}},
		{{Name: "v1.1-000000-000500-bodies.seg"}},
	}
	for _, c := range cases {
		require.NoError(t, v.ValidateStep(context.Background(), c),
			"expected short-circuit on input %v", c)
	}
}

func TestHeaderChainValidator_HeadersFileRequiresDB(t *testing.T) {
	t.Parallel()
	v := HeaderChainValidator{}
	files := []*snapshot.FileEntry{
		{Name: "v1.1-000000-000500-headers.seg"},
	}
	err := v.ValidateStep(context.Background(), files)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil DB")
}

func TestTxRootValidator_NonTxFilesShortCircuit(t *testing.T) {
	t.Parallel()
	v := TxRootValidator{}
	cases := [][]*snapshot.FileEntry{
		{{Name: "v1.1-000000-000500-headers.seg"}},
		{{Name: "v1.1-000000-000500-bodies.seg"}},
		{{Domain: snapshot.DomainAccounts, Name: "v1.1-accounts.0-128.kv"}},
	}
	for _, c := range cases {
		require.NoError(t, v.ValidateStep(context.Background(), c),
			"expected short-circuit on input %v", c)
	}
}

func TestTxRootValidator_TxFileRequiresDB(t *testing.T) {
	t.Parallel()
	v := TxRootValidator{}
	files := []*snapshot.FileEntry{
		{Name: "v1.1-000000-000500-transactions.seg"},
	}
	err := v.ValidateStep(context.Background(), files)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil DB")
}

func TestReceiptRootValidator_NonReceiptDomainShortCircuits(t *testing.T) {
	t.Parallel()
	v := ReceiptRootValidator{}
	for _, domain := range []snapshot.Domain{
		snapshot.DomainAccounts, snapshot.DomainStorage, snapshot.DomainCode, snapshot.DomainCommitment, "",
	} {
		files := []*snapshot.FileEntry{{Domain: domain, FromStep: 0, ToStep: 256}}
		require.NoError(t, v.ValidateStep(context.Background(), files),
			"expected short-circuit on domain %v", domain)
	}
}

func TestReceiptRootValidator_ReceiptDomainRequiresDeps(t *testing.T) {
	t.Parallel()
	// PersistReceipts must be set, else the validator skips before the
	// dep checks (historical receipts not retained → nothing to check).
	v := ReceiptRootValidator{PersistReceipts: true}
	files := []*snapshot.FileEntry{
		{Domain: snapshot.DomainReceipt, FromStep: 0, ToStep: 256},
	}
	err := v.ValidateStep(context.Background(), files)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil DB")
}

// TestReceiptRootValidator_SkipsWhenReceiptsNotPersisted pins the
// minimal-mode contract: with --persist.receipts off there is no
// historical RCache to recompute against, so the receipt-root check is
// skipped (return nil) — receipt files are optional and an
// unvalidatable optional file is not a validation failure. The skip
// happens before the dep checks, so even a zero-value validator passes.
func TestReceiptRootValidator_SkipsWhenReceiptsNotPersisted(t *testing.T) {
	t.Parallel()
	v := ReceiptRootValidator{} // PersistReceipts false
	files := []*snapshot.FileEntry{
		{Domain: snapshot.DomainReceipt, FromStep: 0, ToStep: 256},
	}
	require.NoError(t, v.ValidateStep(context.Background(), files),
		"receipt validator must skip (not fail) when receipts are not persisted")
}
