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

package snapshotsync

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

func TestTypedKeepsPublishedCaplinEntriesAndDropsUnknownTypes(t *testing.T) {
	published := map[string]struct{}{}
	sc := bufio.NewScanner(bytes.NewReader(mainnetPreverifiedFixture))
	for sc.Scan() {
		name, _, ok := strings.Cut(sc.Text(), " = ")
		if !ok {
			continue
		}
		name = strings.Trim(name, "'")
		if strings.HasPrefix(name, "caplin/") {
			published[name] = struct{}{}
		}
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, published, "fixture has no caplin entries to check the window against")

	cfg, ok := snapcfg.KnownCfg(networkname.Mainnet)
	require.True(t, ok)
	items := make(map[string]struct{}, len(cfg.Preverified.Items))
	for _, item := range cfg.Preverified.Items {
		items[item.Name] = struct{}{}
	}

	require.Contains(t, items, "caplin/v1.1-000000-010500-BlockRoot.seg")
	require.Contains(t, items, "caplin/v1.1-000000-011200-PendingDeposits.seg")
	require.NotContains(t, items, "caplin/v1.1-000000-010500-BlockProposers.seg")

	for name := range published {
		if strings.Contains(name, "-BlockProposers.") {
			continue
		}
		require.Contains(t, items, name, "published known Caplin entry was dropped")
	}
}
