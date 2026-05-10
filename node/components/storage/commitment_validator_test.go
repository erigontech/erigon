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

	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// CommitmentDomainValidator full-path testing requires a real
// TemporalRoDB with an aggregator that has a populated commitment
// domain — substantial fixture setup. The validator's full path is
// exercised end-to-end via the hoodi smoke run on a real
// publisher/consumer pair (see docs/plans/20260504-v2-operational-
// guide.md). These unit tests cover the shape: short-circuit on
// non-commitment domains, nil-safety on dependencies.

func TestCommitmentDomainValidator_NonCommitmentStepIsNoOp(t *testing.T) {
	t.Parallel()
	// Validator with nil dependencies — would panic if it tried to
	// open a tx. Non-commitment domains must short-circuit before
	// reaching the DB. The validator introspects files[0].Domain.
	v := CommitmentDomainValidator{}

	for _, domain := range []snapshot.Domain{
		snapshot.DomainAccounts,
		snapshot.DomainStorage,
		snapshot.DomainCode,
	} {
		files := []*snapshot.FileEntry{
			{Domain: domain, FromStep: 0, ToStep: 256},
		}
		err := v.ValidateStep(context.Background(), files)
		require.NoError(t, err,
			"non-commitment domain (%v) must short-circuit without reaching DB", domain)
	}
	// Empty file list also short-circuits.
	require.NoError(t, v.ValidateStep(context.Background(), nil))
}

func TestCommitmentDomainValidator_CommitmentStepRequiresDB(t *testing.T) {
	t.Parallel()
	// Commitment step with nil DB → explicit error, not a panic.
	v := CommitmentDomainValidator{}
	files := []*snapshot.FileEntry{
		{Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 256},
	}
	err := v.ValidateStep(context.Background(), files)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil DB")
}

func TestCommitmentDomainValidator_Name(t *testing.T) {
	t.Parallel()
	v := CommitmentDomainValidator{}
	require.Equal(t, "commitment_domain_state_at_end", v.Name())
}

// fakeStepValidator simulates the per-file accept/reject pattern of
// CommitmentDomainValidator without the DB infrastructure. Used to
// exercise seedLatestCommitmentBinding's candidate-iteration logic.
type fakeStepValidator struct {
	// acceptStep is the ToStep value the validator accepts; everything
	// else returns an error. Mirrors the production case where some
	// commitment files (e.g. mid-block written ones) are rejected.
	acceptStep uint64
	// fired is the names the validator was actually invoked on, in
	// order — lets tests assert iteration order.
	fired []string
	// onAccept (optional) registers a binding when validation passes,
	// mirroring the real CommitmentDomainValidator's side effect.
	onAccept func(toStep uint64)
}

func (f *fakeStepValidator) ValidateStep(_ context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 {
		return nil
	}
	f.fired = append(f.fired, files[0].Name)
	if files[0].ToStep == f.acceptStep {
		if f.onAccept != nil {
			f.onAccept(files[0].ToStep)
		}
		return nil
	}
	return errStepRejected
}

var errStepRejected = simpleErr("rejected")

type simpleErr string

func (e simpleErr) Error() string { return string(e) }

// TestSeedLatestCommitmentBinding_FallsBackThroughCandidates exercises
// the candidate-iteration logic: bootstrap seeder must try commitment
// files in descending ToStep order and stop at the first one that
// validates. On the 2026-05-06 mainnet publisher run, the latest two
// commitment files were rejected (mid-block written) and the third
// passed — without the fallback no binding got registered, so the
// block-step wait gate stayed closed for every block file.
func TestSeedLatestCommitmentBinding_FallsBackThroughCandidates(t *testing.T) {
	t.Parallel()

	inv := snapshot.NewInventory()
	for _, e := range []*snapshot.FileEntry{
		{Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 256, Name: "v1.1-commitment.0-256.kv"},
		{Domain: snapshot.DomainCommitment, FromStep: 256, ToStep: 512, Name: "v1.1-commitment.256-512.kv"},
		{Domain: snapshot.DomainCommitment, FromStep: 512, ToStep: 768, Name: "v1.1-commitment.512-768.kv"},
	} {
		require.NoError(t, inv.AddFile(e))
	}

	v := &fakeStepValidator{
		acceptStep: 512, // middle candidate accepts
		onAccept: func(toStep uint64) {
			inv.RegisterStepBlockBoundary(toStep, 100_000)
		},
	}
	seedLatestCommitmentBinding(context.Background(), inv, v, nil)

	// Iteration is descending by ToStep: 768 (rejected) → 512 (accepted).
	// 256 must NOT be tried because 512 succeeded.
	require.Equal(t,
		[]string{"v1.1-commitment.512-768.kv", "v1.1-commitment.256-512.kv"},
		v.fired,
		"seeder should iterate descending and stop on first success")
	require.Len(t, inv.StepBlockBoundaries(), 1,
		"successful candidate registers exactly one binding")
}

// TestSeedLatestCommitmentBinding_AllCandidatesFail: when no candidate
// validates, the seeder must NOT register any binding (block-step gate
// stays closed; lifecycle's normal flow is responsible for producing a
// fresh commitment).
func TestSeedLatestCommitmentBinding_AllCandidatesFail(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{
		Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 256,
		Name: "v1.1-commitment.0-256.kv",
	}))

	v := &fakeStepValidator{acceptStep: 9999} // matches nothing in inventory
	seedLatestCommitmentBinding(context.Background(), inv, v, nil)

	require.Empty(t, inv.StepBlockBoundaries(),
		"no binding should be registered when every candidate fails")
}

// TestSeedLatestCommitmentBinding_EmptyInventoryIsNoOp: a fresh
// consumer datadir has no commitment files yet — seeder must
// short-circuit cleanly.
func TestSeedLatestCommitmentBinding_EmptyInventoryIsNoOp(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	v := &fakeStepValidator{}
	seedLatestCommitmentBinding(context.Background(), inv, v, nil)
	require.Empty(t, v.fired)
	require.Empty(t, inv.StepBlockBoundaries())
}

// TestPausedCommitmentCache_GetPutForget exercises the cache's basic
// contract.
func TestPausedCommitmentCache_GetPutForget(t *testing.T) {
	t.Parallel()
	c := NewPausedCommitmentCache()

	_, ok := c.Get("missing.kv")
	require.False(t, ok)

	c.Put("a.kv", integrity.CommitmentRootInfo{TxNum: 8970, BlockNum: 25049601, BlockMaxTxNum: 1234567890})
	got, ok := c.Get("a.kv")
	require.True(t, ok)
	require.Equal(t, uint64(25049601), got.BlockNum)

	c.Forget("a.kv")
	_, ok = c.Get("a.kv")
	require.False(t, ok)
}

// TestPausedCommitmentCache_NilSafe verifies the nil receiver no-ops
// (the cache is optional; tests/tools without one must not crash).
func TestPausedCommitmentCache_NilSafe(t *testing.T) {
	t.Parallel()
	var c *PausedCommitmentCache
	_, ok := c.Get("any")
	require.False(t, ok)
	c.Put("any", integrity.CommitmentRootInfo{})
	c.Forget("any")
}
