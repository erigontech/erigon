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
	// Validator with nil dependencies — would panic if it tried to
	// open a tx. Non-commitment domains must short-circuit before
	// reaching the DB.
	v := CommitmentDomainValidator{}

	for _, key := range []snapshot.StepKey{
		{FromStep: 0, ToStep: 256, Domain: snapshot.DomainAccounts},
		{FromStep: 0, ToStep: 256, Domain: snapshot.DomainStorage},
		{FromStep: 0, ToStep: 256, Domain: snapshot.DomainCode},
		{}, // zero key (singleton)
	} {
		group := snapshot.StepGroup{Key: key}
		err := v.ValidateStep(context.Background(), group)
		require.NoError(t, err,
			"non-commitment domain (%v) must short-circuit without reaching DB", key)
	}
}

func TestCommitmentDomainValidator_CommitmentStepRequiresDB(t *testing.T) {
	// Commitment step with nil DB → explicit error, not a panic.
	v := CommitmentDomainValidator{}
	group := snapshot.StepGroup{
		Key: snapshot.StepKey{FromStep: 0, ToStep: 256, Domain: snapshot.DomainCommitment},
	}
	err := v.ValidateStep(context.Background(), group)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil DB")
}

func TestCommitmentDomainValidator_Name(t *testing.T) {
	v := CommitmentDomainValidator{}
	require.Equal(t, "commitment_domain_state_at_end", v.Name())
}
