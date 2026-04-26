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

package harness

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestLoadPreverifiedClassification feeds a representative subset of
// real Hoodi preverified.toml entries through the parser and asserts each
// is classified into the right role. Locks the categorisation so the
// full-replication test can rely on (Role, Kind, Domain, FromStep, ToStep)
// without re-deriving from filenames.
func TestLoadPreverifiedClassification(t *testing.T) {
	dir := t.TempDir()
	body := `
'erigondb.toml'                                      = '49fd79689b9cc4e5dc7ce7b2b72f2f9bf3c668b7'
'salt-blocks.txt'                                    = '29f27ab4e68583bf65405212f7bb647315aee42f'
'salt-state.txt'                                     = '0113ec56c7c0d72783dbb5738becffa9c4c43d69'
'v1.1-000000-000100-headers.seg'                     = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
'v1.1-000000-000100-bodies.seg'                      = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
'v1.1-000000-000100-transactions.seg'                = 'cccccccccccccccccccccccccccccccccccccccc'
'v1.1-000000-000100-transactions-to-block.idx'       = 'dddddddddddddddddddddddddddddddddddddddd'
'v1.1-000000-000100-headers.idx'                     = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
'caplin/v1.1-000000-000010-beaconblocks.seg'         = 'caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1'
'caplin/v1.1-000000-000010-beaconblocks.idx'         = 'caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2'
'caplin/v1.1-000000-000050-ActiveValidatorIndicies.seg' = 'caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3'
'caplin/v1.1-000000-000050-BlockRoot.idx'            = 'caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4'
'domain/v1.1-accounts.0-128.kv'                      = '0000000000000000000000000000000000000001'
'domain/v1.1-accounts.0-128.kvi'                     = '0000000000000000000000000000000000000002'
'domain/v1.1-accounts.0-128.bt'                      = '0000000000000000000000000000000000000003'
'domain/v1.1-accounts.0-128.kvei'                    = '0000000000000000000000000000000000000004'
'history/v1.1-accounts.0-128.v'                      = '0000000000000000000000000000000000000005'
'idx/v1.1-accounts.0-128.ef'                         = '0000000000000000000000000000000000000006'
'accessor/v1.1-accounts.0-128.vi'                    = '0000000000000000000000000000000000000007'
'accessor/v1.1-accounts.0-128.efi'                   = '0000000000000000000000000000000000000008'
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "preverified.toml"), []byte(body), 0o644))

	entries, err := LoadPreverified(dir)
	require.NoError(t, err)

	byPath := make(map[string]PreverifiedEntry, len(entries))
	for _, e := range entries {
		byPath[e.RelPath] = e
	}

	type expect struct {
		role     PreverifiedRole
		kind     snapshot.FileKind
		domain   snapshot.Domain
		from, to uint64
	}
	cases := map[string]expect{
		"erigondb.toml":                              {RoleMeta, snapshot.KindMeta, "", 0, 0},
		"salt-blocks.txt":                            {RoleSalt, snapshot.KindSalt, "", 0, 0},
		"salt-state.txt":                             {RoleSalt, snapshot.KindSalt, "", 0, 0},
		"v1.1-000000-000100-headers.seg":             {RoleBlockSeg, "", "", 0, 100},
		"v1.1-000000-000100-bodies.seg":              {RoleBlockSeg, "", "", 0, 100},
		"v1.1-000000-000100-transactions.seg":        {RoleBlockSeg, "", "", 0, 100},
		"v1.1-000000-000100-transactions-to-block.idx": {RoleDerivedBlockIdx, "", "", 0, 100},
		"v1.1-000000-000100-headers.idx":             {RoleDerivedBlockIdx, "", "", 0, 100},
		"caplin/v1.1-000000-000010-beaconblocks.seg":            {RoleCaplinSeg, snapshot.KindCaplin, "", 0, 10},
		"caplin/v1.1-000000-000010-beaconblocks.idx":            {RoleDerivedCaplinIdx, "", "", 0, 10},
		"caplin/v1.1-000000-000050-ActiveValidatorIndicies.seg": {RoleCaplinSeg, snapshot.KindCaplin, "", 0, 50},
		"caplin/v1.1-000000-000050-BlockRoot.idx":               {RoleDerivedCaplinIdx, "", "", 0, 50},
		"domain/v1.1-accounts.0-128.kv":              {RoleDomainKV, snapshot.KindKV, snapshot.DomainAccounts, 0, 128},
		"domain/v1.1-accounts.0-128.kvi":             {RoleDerivedAccessor, "", snapshot.DomainAccounts, 0, 128},
		"domain/v1.1-accounts.0-128.bt":              {RoleDerivedAccessor, "", snapshot.DomainAccounts, 0, 128},
		"domain/v1.1-accounts.0-128.kvei":            {RoleDerivedAccessor, "", snapshot.DomainAccounts, 0, 128},
		"history/v1.1-accounts.0-128.v":              {RoleDomainHistory, snapshot.KindHistory, snapshot.DomainAccounts, 0, 128},
		"idx/v1.1-accounts.0-128.ef":                 {RoleDomainIdx, snapshot.KindIdx, snapshot.DomainAccounts, 0, 128},
		"accessor/v1.1-accounts.0-128.vi":            {RoleDerivedAccessor, "", snapshot.DomainAccounts, 0, 128},
		"accessor/v1.1-accounts.0-128.efi":           {RoleDerivedAccessor, "", snapshot.DomainAccounts, 0, 128},
	}

	for path, want := range cases {
		got, ok := byPath[path]
		require.True(t, ok, "preverified.toml entry %q missing from parse", path)
		require.Equal(t, want.role, got.Role, "%s role", path)
		require.Equal(t, want.kind, got.Kind, "%s kind", path)
		require.Equal(t, want.domain, got.Domain, "%s domain", path)
		require.Equal(t, want.from, got.FromStep, "%s from", path)
		require.Equal(t, want.to, got.ToStep, "%s to", path)
	}

	// Primary classification — the test's open-loop transfer set must be
	// exactly the V2-covered roles.
	primaryCount := 0
	for _, e := range entries {
		if e.Role.IsPrimary() {
			primaryCount++
		}
	}
	// Of the 20 fixture lines: 1 erigondb.toml + 2 salt + 3 block .seg +
	// 2 caplin .seg + 1 .kv + 1 .v + 1 .ef = 11 primaries.
	require.Equal(t, 11, primaryCount)
}
