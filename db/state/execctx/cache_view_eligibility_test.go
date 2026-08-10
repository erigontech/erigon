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

package execctx

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type exactDomainViewStub struct {
	exact   map[kv.Domain]bool
	checked []kv.Domain
}

func (s *exactDomainViewStub) HasExactDomainVisibleEnd(domain kv.Domain) bool {
	s.checked = append(s.checked, domain)
	return s.exact[domain]
}

func TestCacheViewEligibleUsesExactViewAvailability(t *testing.T) {
	exact := map[kv.Domain]bool{
		kv.AccountsDomain: true,
		kv.StorageDomain:  true,
		kv.CodeDomain:     true,
	}
	debug := &exactDomainViewStub{exact: exact}
	require.True(t, cacheViewEligible(debug, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain))
	require.Equal(t, []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain}, debug.checked)

	exact[kv.StorageDomain] = false
	debug.checked = nil
	require.False(t, cacheViewEligible(debug, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain))
	require.Equal(t, []kv.Domain{kv.AccountsDomain, kv.StorageDomain}, debug.checked)
}
