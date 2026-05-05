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
