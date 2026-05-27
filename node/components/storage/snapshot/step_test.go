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

func TestStepKey_GroupsByFromToDomain(t *testing.T) {
	t.Parallel()
	a := &FileEntry{Name: "a", FromStep: 0, ToStep: 256, Domain: DomainAccounts}
	b := &FileEntry{Name: "b", FromStep: 0, ToStep: 256, Domain: DomainAccounts}
	c := &FileEntry{Name: "c", FromStep: 256, ToStep: 512, Domain: DomainAccounts}
	d := &FileEntry{Name: "d", FromStep: 0, ToStep: 256, Domain: DomainStorage}
	e := &FileEntry{Name: "e", FromStep: 0, ToStep: 256}

	require.Equal(t, a.StepKey(), b.StepKey(), "same range + domain → same key")
	require.NotEqual(t, a.StepKey(), c.StepKey(), "different range → different key")
	require.NotEqual(t, a.StepKey(), d.StepKey(), "different domain → different key")
	require.NotEqual(t, a.StepKey(), e.StepKey(), "block (empty domain) vs state → different key")
}

func TestStepKey_Zero(t *testing.T) {
	t.Parallel()
	caplin := &FileEntry{Name: "caplin/x.seg", Kind: KindCaplin}
	meta := &FileEntry{Name: "erigondb.toml", Kind: KindMeta}
	salt := &FileEntry{Name: "salt-state.txt", Kind: KindSalt}
	for _, f := range []*FileEntry{caplin, meta, salt} {
		require.True(t, f.StepKey().IsZero(),
			"non-stepped file %q should yield zero StepKey", f.Name)
	}
}

func TestIsMinimum_StateStep(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		domain   Domain
		fileName string
		expect   bool
	}{
		{"kv primary", DomainAccounts, "v1.0-accounts.0-256.kv", true},
		{"kvi accessor", DomainAccounts, "v1.0-accounts.0-256.kvi", true},
		{"bt accessor", DomainAccounts, "v1.0-accounts.0-256.bt", true},
		{"history v file", DomainAccounts, "v1.0-accountsHistory.0-256.v", false},
		{"history ef file", DomainAccounts, "v1.0-accountsHistory.0-256.ef", false},
		{"history efi file", DomainAccounts, "v1.0-accountsHistory.0-256.efi", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &FileEntry{Name: tc.fileName, Domain: tc.domain, FromStep: 0, ToStep: 256}
			require.Equal(t, tc.expect, f.IsMinimum())
		})
	}
}

func TestIsMinimum_BlockStep(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		fileName string
		expect   bool
	}{
		{"headers seg", "v1.1-001000-001001-headers.seg", true},
		{"headers idx", "v1.1-001000-001001-headers.idx", true},
		{"bodies seg", "v1.1-001000-001001-bodies.seg", false},
		{"bodies idx", "v1.1-001000-001001-bodies.idx", false},
		{"transactions seg", "v1.1-001000-001001-transactions.seg", false},
		{"transactions idx", "v1.1-001000-001001-transactions.idx", false},
		{"transactions efi", "v1.1-001000-001001-transactions.efi", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &FileEntry{Name: tc.fileName, FromStep: 1000, ToStep: 1001}
			require.Equal(t, tc.expect, f.IsMinimum())
		})
	}
}

func TestIsMinimum_NonStepped(t *testing.T) {
	t.Parallel()
	for _, f := range []*FileEntry{
		{Name: "caplin/x.seg", Kind: KindCaplin},
		{Name: "erigondb.toml", Kind: KindMeta},
		{Name: "salt-state.txt", Kind: KindSalt},
		nil,
	} {
		require.False(t, f.IsMinimum(),
			"non-stepped or nil file should not be flagged minimum")
	}
}

func TestFilesAtStep_GroupsCorrectly(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	files := []*FileEntry{
		{Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts, FromStep: 0, ToStep: 256, Local: true},
		{Name: "v1.0-accounts.0-256.kvi", Domain: DomainAccounts, FromStep: 0, ToStep: 256, Local: true},
		{Name: "v1.0-accountsHistory.0-256.v", Domain: DomainAccounts, FromStep: 0, ToStep: 256, Local: true},
		{Name: "v1.0-accounts.256-512.kv", Domain: DomainAccounts, FromStep: 256, ToStep: 512, Local: true},
		{Name: "v1.0-storage.0-256.kv", Domain: DomainStorage, FromStep: 0, ToStep: 256, Local: true},
	}
	for _, f := range files {
		inv.AddFile(f)
	}

	// State step (accounts, 0-256) — three files.
	g := inv.FilesAtStep(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts})
	require.Len(t, g.Files, 3)
	require.Len(t, g.Minimum(), 2, "kv + kvi are minimum; .v is extras")
	require.Len(t, g.Extras(), 1)

	// Different range: only the .256-512 file.
	g2 := inv.FilesAtStep(StepKey{FromStep: 256, ToStep: 512, Domain: DomainAccounts})
	require.Len(t, g2.Files, 1)

	// Different domain: only the storage file.
	g3 := inv.FilesAtStep(StepKey{FromStep: 0, ToStep: 256, Domain: DomainStorage})
	require.Len(t, g3.Files, 1)

	// No match.
	g4 := inv.FilesAtStep(StepKey{FromStep: 999, ToStep: 1000, Domain: DomainAccounts})
	require.Empty(t, g4.Files)
}

func TestFilesAtStep_BlockGroup(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	for _, f := range []*FileEntry{
		{Name: "v1.1-001000-001001-headers.seg", FromStep: 1000, ToStep: 1001, Local: true},
		{Name: "v1.1-001000-001001-headers.idx", FromStep: 1000, ToStep: 1001, Local: true},
		{Name: "v1.1-001000-001001-bodies.seg", FromStep: 1000, ToStep: 1001, Local: true},
		{Name: "v1.1-001000-001001-transactions.seg", FromStep: 1000, ToStep: 1001, Local: true},
	} {
		inv.AddFile(f)
	}
	g := inv.FilesAtStep(StepKey{FromStep: 1000, ToStep: 1001})
	require.Len(t, g.Files, 4)
	require.Len(t, g.Minimum(), 2, "headers.seg + headers.idx are minimum")
	require.Len(t, g.Extras(), 2, "bodies.seg + transactions.seg are extras")
}

func TestFilesAtStep_ZeroKeyReturnsEmpty(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "erigondb.toml", Kind: KindMeta, Local: true})
	g := inv.FilesAtStep(StepKey{})
	require.Empty(t, g.Files,
		"zero StepKey is not a valid grouping; non-stepped files are singletons")
}

func TestStepGroup_AllAtState(t *testing.T) {
	t.Parallel()
	g := StepGroup{Files: []*FileEntry{
		{Name: "a", State: LifecycleIndexed},
		{Name: "b", State: LifecycleAdvertisable},
	}}
	require.True(t, g.AllAtState(LifecycleIndexed))
	require.False(t, g.AllAtState(LifecycleAdvertisable),
		"a is at Indexed which is < Advertisable")

	gIncomplete := StepGroup{Files: []*FileEntry{
		{Name: "a", State: LifecycleDownloaded},
		{Name: "b", State: LifecycleIndexed},
	}}
	require.False(t, gIncomplete.AllAtState(LifecycleIndexed),
		"a is at Downloaded which is < Indexed")

	require.True(t, StepGroup{}.AllAtState(LifecycleIndexed),
		"empty group is vacuously satisfied")
}

func TestAdvanceStep_AtomicAdvance(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	for _, f := range []*FileEntry{
		{Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts, FromStep: 0, ToStep: 256, State: LifecycleIndexed},
		{Name: "v1.0-accounts.0-256.kvi", Domain: DomainAccounts, FromStep: 0, ToStep: 256, State: LifecycleIndexed},
	} {
		inv.AddFile(f)
	}
	advanced := inv.AdvanceStep(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts}, LifecycleAdvertisable)
	require.ElementsMatch(t, []string{
		"v1.0-accounts.0-256.kv",
		"v1.0-accounts.0-256.kvi",
	}, advanced)

	// Verify state actually transitioned.
	for _, name := range advanced {
		got, _ := inv.LifecycleState(name)
		require.Equal(t, LifecycleAdvertisable, got)
	}
}

func TestAdvanceStep_Idempotent(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts,
		FromStep: 0, ToStep: 256, State: LifecycleAdvertisable,
	})
	advanced := inv.AdvanceStep(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts}, LifecycleAdvertisable)
	require.Empty(t, advanced,
		"already-at-target files don't get re-advanced or re-notified")
}

func TestPopulateFromName_StateFile(t *testing.T) {
	t.Parallel()
	e := &FileEntry{Name: "v1.0-accounts.0-256.kv"}
	require.True(t, PopulateFromName(e))
	require.Equal(t, uint64(0), e.FromStep)
	require.Equal(t, uint64(256), e.ToStep)
	require.Equal(t, DomainAccounts, e.Domain)
	require.Equal(t, KindKV, e.Kind)
}

func TestPopulateFromName_HistoryFile(t *testing.T) {
	t.Parallel()
	e := &FileEntry{Name: "v1.0-accountsHistory.0-256.v"}
	require.True(t, PopulateFromName(e))
	require.Equal(t, DomainAccounts, e.Domain,
		"History suffix is stripped for domain mapping")
	require.Equal(t, KindHistory, e.Kind)
}

func TestPopulateFromName_BlockFile(t *testing.T) {
	t.Parallel()
	e := &FileEntry{Name: "v1.1-000900-001000-headers.seg"}
	require.True(t, PopulateFromName(e))
	require.Equal(t, Domain(""), e.Domain, "block files have empty Domain")
	// Block files populate the block-axis (FromBlock/ToBlock) — NOT
	// the step axis. FromStep/ToStep stay zero until a commitment-
	// derived (step, block) binding establishes the step.
	require.Equal(t, uint64(0), e.FromStep,
		"block files don't carry step until commitment binds them")
	require.Equal(t, uint64(0), e.ToStep,
		"block files don't carry step until commitment binds them")
	require.Equal(t, uint64(900_000), e.FromBlock,
		"block files populate the block-axis")
	require.Equal(t, uint64(1_000_000), e.ToBlock,
		"block files populate the block-axis")
	require.Equal(t, KindKV, e.Kind, ".seg without caplin/ → KindKV")
}

func TestPopulateFromName_PreservesExistingFields(t *testing.T) {
	t.Parallel()
	e := &FileEntry{
		Name:     "v1.0-accounts.0-256.kv",
		Domain:   DomainStorage, // wrong-but-explicit; preserved
		FromStep: 99,
		ToStep:   200,
	}
	PopulateFromName(e)
	require.Equal(t, DomainStorage, e.Domain, "explicit Domain preserved")
	require.Equal(t, uint64(99), e.FromStep, "explicit FromStep preserved")
	require.Equal(t, uint64(200), e.ToStep, "explicit ToStep preserved")
}

func TestPopulateFromName_NilOrEmpty(t *testing.T) {
	t.Parallel()
	require.False(t, PopulateFromName(nil))
	require.False(t, PopulateFromName(&FileEntry{}))
}

// TestPopulateFromName_V4StateFileSkipsStepAxis pins the v4.0 dispatch:
// PopulateFromName has no stepSize so it cannot convert v4.0 raw
// txnums to step indices. Rather than silently writing the txnum
// value into FromStep/ToStep (the legacy bug this fix guards), it
// leaves the step axis at zero so the caller using
// PopulateFromNameWithStepSize is the only path that populates v4.0
// step coords.
func TestPopulateFromName_V4StateFileSkipsStepAxis(t *testing.T) {
	t.Parallel()
	e := &FileEntry{Name: "v4.0-accounts.0-128000.kv"}
	populated := PopulateFromName(e)
	require.True(t, populated, "Kind and Domain still populate even without step axis")
	require.Equal(t, DomainAccounts, e.Domain)
	require.Equal(t, KindKV, e.Kind)
	require.Equal(t, uint64(0), e.FromStep,
		"v4.0 raw-txnum names must NOT silently populate FromStep — stepSize unknown")
	require.Equal(t, uint64(0), e.ToStep,
		"v4.0 raw-txnum names must NOT silently populate ToStep — stepSize unknown")
}

// TestPopulateFromNameWithStepSize_V4Dispatch confirms the v4.0
// version-aware variant converts raw txnums to step indices via
// the supplied stepSize.
func TestPopulateFromNameWithStepSize_V4Dispatch(t *testing.T) {
	t.Parallel()
	const stepSize = 1000
	e := &FileEntry{Name: "v4.0-accounts.0-128000.kv"}
	require.True(t, PopulateFromNameWithStepSize(e, stepSize))
	require.Equal(t, uint64(0), e.FromStep, "0 / 1000 = 0")
	require.Equal(t, uint64(128), e.ToStep, "128000 / 1000 = 128")
}

// TestPopulateFromNameWithStepSize_LegacyUnchanged confirms the v4.0
// variant doesn't alter the legacy step-indexed path — the same
// (FromStep, ToStep) as PopulateFromName returns.
func TestPopulateFromNameWithStepSize_LegacyUnchanged(t *testing.T) {
	t.Parallel()
	e := &FileEntry{Name: "v1.0-accounts.0-256.kv"}
	require.True(t, PopulateFromNameWithStepSize(e, 1000))
	require.Equal(t, uint64(0), e.FromStep)
	require.Equal(t, uint64(256), e.ToStep,
		"legacy v1.0 files are already step-indexed; stepSize doesn't divide them")
}

// TestPopulateFromNameWithStepSize_ZeroStepSize confirms the v4.0
// variant degrades to PopulateFromName-equivalent behavior when
// stepSize==0: legacy files populate normally; v4.0 files skip the
// step axis (can't divide by zero).
func TestPopulateFromNameWithStepSize_ZeroStepSize(t *testing.T) {
	t.Parallel()
	legacy := &FileEntry{Name: "v1.0-accounts.0-256.kv"}
	require.True(t, PopulateFromNameWithStepSize(legacy, 0))
	require.Equal(t, uint64(256), legacy.ToStep, "legacy files don't need stepSize")

	v4 := &FileEntry{Name: "v4.0-accounts.0-128000.kv"}
	require.True(t, PopulateFromNameWithStepSize(v4, 0))
	require.Equal(t, uint64(0), v4.ToStep,
		"v4.0 files with stepSize==0 leave step axis unset (no divide-by-zero)")
}

// TestRelPathForName_Idempotent: the inventory mixes basenames and
// already-relative names (disk-scan path uses basenames; legacy
// retire OnFilesChange uses subdir-prefixed names). RelPathForName
// must converge both forms on the same output, otherwise the bridge
// subscriber double-prefixes (idx/idx/foo.ef) and the downloader
// fails to find the file.
func TestRelPathForName_Idempotent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in, want string
	}{
		// Domain primaries + their in-domain accessors all live in domain/.
		{"v1.1-commitment.0-256.kv", "domain/v1.1-commitment.0-256.kv"},
		{"domain/v1.1-commitment.0-256.kv", "domain/v1.1-commitment.0-256.kv"}, // already relative
		{"v2.0-accounts.0-256.kvi", "domain/v2.0-accounts.0-256.kvi"},
		{"v2.0-accounts.0-256.kvei", "domain/v2.0-accounts.0-256.kvei"},
		{"v2.0-accounts.0-256.bt", "domain/v2.0-accounts.0-256.bt"},
		// Idx + history primaries.
		{"v3.0-logaddrs.8956-8958.ef", "idx/v3.0-logaddrs.8956-8958.ef"},
		{"idx/v3.0-logaddrs.8956-8958.ef", "idx/v3.0-logaddrs.8956-8958.ef"}, // already relative
		{"v2.0-accounts.8956-8957.v", "history/v2.0-accounts.8956-8957.v"},
		{"history/v2.0-accounts.8956-8957.v", "history/v2.0-accounts.8956-8957.v"},
		// History accessors (.vi) live in accessor/.
		{"v1.1-code.0-256.vi", "accessor/v1.1-code.0-256.vi"},
		{"accessor/v1.1-code.0-256.vi", "accessor/v1.1-code.0-256.vi"},
		// Top-level (block files + singletons).
		{"v1.1-025020-025030-headers.seg", "v1.1-025020-025030-headers.seg"},
		{"erigondb.toml", "erigondb.toml"},
	} {
		require.Equal(t, tc.want, RelPathForName(tc.in), "input=%q", tc.in)
		// Double-application is also idempotent.
		require.Equal(t, tc.want, RelPathForName(RelPathForName(tc.in)), "double-call input=%q", tc.in)
	}
}

// TestPathForName_Idempotent: same idempotency property for the
// snap-dir-joined form used by AllFilesPresent.
func TestPathForName_Idempotent(t *testing.T) {
	t.Parallel()
	const root = "/snap"
	for _, tc := range []struct {
		in, want string
	}{
		{"foo.kv", "/snap/domain/foo.kv"},
		{"domain/foo.kv", "/snap/domain/foo.kv"},
		{"foo.kvi", "/snap/domain/foo.kvi"},
		{"foo.bt", "/snap/domain/foo.bt"},
		{"foo.ef", "/snap/idx/foo.ef"},
		{"idx/foo.ef", "/snap/idx/foo.ef"},
		{"foo.vi", "/snap/accessor/foo.vi"},
		{"foo.seg", "/snap/foo.seg"},
	} {
		require.Equal(t, tc.want, PathForName(root, tc.in), "input=%q", tc.in)
	}
}

func TestAdvanceStep_ZeroKeyNoOp(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "erigondb.toml", Kind: KindMeta, State: LifecycleIndexed})
	advanced := inv.AdvanceStep(StepKey{}, LifecycleAdvertisable)
	require.Empty(t, advanced)
}
