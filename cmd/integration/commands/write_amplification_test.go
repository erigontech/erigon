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

package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestParseDomainsFlagIncludesCommitmentBin(t *testing.T) {
	all, err := parseDomainsFlag("all")
	require.NoError(t, err)
	require.Equal(t, []kv.Domain{
		kv.AccountsDomain,
		kv.StorageDomain,
		kv.CodeDomain,
		kv.CommitmentDomain,
		kv.CommitmentBinDomain,
	}, all)

	selected, err := parseDomainsFlag("commitment-bin")
	require.NoError(t, err)
	require.Equal(t, []kv.Domain{kv.CommitmentBinDomain}, selected)
}
