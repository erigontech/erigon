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

package handler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/common"
)

func TestBuilderRouteStoreReaddingSpentRouteDoesNotRearm(t *testing.T) {
	now := time.Unix(100, 0)
	routes := newBuilderRouteStore(2, time.Minute, func() time.Time { return now })
	root := common.Hash{1}
	url := "https://builder.example"

	require.True(t, routes.Add(root, url))
	claimID, claimed := routes.Claim(root, url)
	require.True(t, claimed)
	routes.Complete(root, url, claimID, true)
	require.True(t, routes.Add(root, url))
	_, claimed = routes.Claim(root, url)
	require.False(t, claimed)
}

func TestBuilderWinningResponseFailsWhenRouteCapacityIsFull(t *testing.T) {
	handler := &ApiHandler{builderRoutes: newBuilderRouteStore(1, time.Minute, time.Now)}
	require.True(t, handler.builderRoutes.Add(common.Hash{1}, "https://one.example"))
	recorder := httptest.NewRecorder()

	err := handler.setBuilderRouteHeader(recorder, common.Hash{2}, "https://two.example")

	require.Error(t, err)
	var endpointErr *beaconhttp.EndpointError
	require.True(t, errors.As(err, &endpointErr))
	require.Equal(t, http.StatusServiceUnavailable, endpointErr.Code)
	require.Empty(t, recorder.Header().Get("Eth-Builder-Url"))
}

func TestBuilderRouteStoreReservationPreventsLateCapacityFailure(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	require.True(t, routes.Reserve())
	require.False(t, routes.Add(common.Hash{1}, "https://other.example"))
	require.True(t, routes.CommitReservation(common.Hash{2}, "https://reserved.example"))
	_, claimed := routes.Claim(common.Hash{2}, "https://reserved.example")
	require.True(t, claimed)
}

func TestBuilderRouteStoreReservationUpgradesSpentUnboundRoute(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)
	root := common.Hash{1}
	url := "https://builder.example"
	require.True(t, routes.Reserve())
	claimID, _, claimed := routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	routes.Complete(root, url, claimID, true)

	require.True(t, routes.CommitReservation(root, url))
	_, claimed = routes.Claim(root, url)
	require.True(t, claimed)
}

func TestBuilderRouteStoreReservationUpgradesInFlightUnboundRoute(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)
	root := common.Hash{1}
	url := "https://builder.example"
	require.True(t, routes.Reserve())
	unboundClaimID, _, claimed := routes.ClaimOrAdd(root, url)
	require.True(t, claimed)

	require.True(t, routes.CommitReservation(root, url))
	trustedClaimID, claimed := routes.Claim(root, url)
	require.True(t, claimed)
	routes.Complete(root, url, unboundClaimID, true)
	routes.Complete(root, url, trustedClaimID, false)
	_, claimed = routes.Claim(root, url)
	require.True(t, claimed)
}

func TestBuilderRouteStoreReservationPreservesInFlightTrustedRoute(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)
	root := common.Hash{1}
	url := "https://builder.example"
	require.True(t, routes.Add(root, url))
	claimID, claimed := routes.Claim(root, url)
	require.True(t, claimed)
	require.True(t, routes.Reserve())

	require.True(t, routes.CommitReservation(root, url))
	_, claimed = routes.Claim(root, url)
	require.False(t, claimed)
	routes.Complete(root, url, claimID, true)
	_, claimed = routes.Claim(root, url)
	require.False(t, claimed)
}

func TestBuilderRouteStoreStaleCompletionDoesNotMatchRecreatedRoute(t *testing.T) {
	now := time.Unix(100, 0)
	routes := newBuilderRouteStore(2, time.Minute, func() time.Time { return now })
	root := common.Hash{1}
	url := "https://builder.example"
	require.True(t, routes.Reserve())
	staleClaimID, _, claimed := routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.True(t, routes.CommitReservation(root, url))
	trustedClaimID, claimed := routes.Claim(root, url)
	require.True(t, claimed)
	routes.Complete(root, url, trustedClaimID, true)

	now = now.Add(time.Second)
	otherRoot := common.Hash{2}
	require.True(t, routes.Add(otherRoot, url))
	otherClaimID, claimed := routes.Claim(otherRoot, url)
	require.True(t, claimed)
	routes.Complete(otherRoot, url, otherClaimID, true)
	require.True(t, routes.Add(common.Hash{3}, url))
	require.True(t, routes.Add(root, url))
	currentClaimID, claimed := routes.Claim(root, url)
	require.True(t, claimed)

	routes.Complete(root, url, staleClaimID, true)
	routes.Complete(root, url, currentClaimID, false)
	_, claimed = routes.Claim(root, url)
	require.True(t, claimed)
}

func TestBuilderRouteStoreReleasedReservationFreesCapacity(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	require.True(t, routes.Reserve())
	routes.ReleaseReservation()
	require.True(t, routes.Add(common.Hash{1}, "https://builder.example"))
}

func TestBuilderRouteStoreAllowsAliasesForSameRoot(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)
	root := common.Hash{1}

	require.True(t, routes.Add(root, "https://one.example"))
	require.True(t, routes.Add(root, "https://two.example"))
	_, claimed := routes.Claim(root, "https://one.example")
	require.True(t, claimed)
	_, claimed = routes.Claim(root, "https://two.example")
	require.True(t, claimed)
}

func TestBuilderRouteStoreClaimOrAddReportsTrustedRoute(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	root := common.Hash{1}
	url := "https://builder.example"
	require.True(t, routes.Add(root, url))

	claimID, trusted, claimed := routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.True(t, trusted)
	routes.Complete(root, url, claimID, false)
	_, trusted, claimed = routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.True(t, trusted)
}

func TestBuilderRouteStoreTrustedClaimDoesNotExtendExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	ttl := time.Minute
	routes := newBuilderRouteStore(1, ttl, func() time.Time { return now })
	root := common.Hash{1}
	url := "https://builder.example"
	require.True(t, routes.Add(root, url))
	now = now.Add(ttl - time.Second)
	claimID, trusted, claimed := routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.True(t, trusted)
	routes.Complete(root, url, claimID, false)

	now = now.Add(time.Second)
	_, trusted, claimed = routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.False(t, trusted)
}

func TestBuilderRouteStoreClaimOrAddIsBoundedAndSingleflight(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	root := common.Hash{1}
	url := "https://builder.example"

	claimID, trusted, claimed := routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.False(t, trusted)
	_, _, claimed = routes.ClaimOrAdd(root, url)
	require.False(t, claimed)
	_, _, claimed = routes.ClaimOrAdd(common.Hash{2}, "https://other.example")
	require.False(t, claimed)
	routes.Complete(root, url, claimID, false)
	claimID, trusted, claimed = routes.ClaimOrAdd(root, url)
	require.True(t, claimed)
	require.False(t, trusted)
	routes.Complete(root, url, claimID, true)
	_, _, claimed = routes.ClaimOrAdd(root, url)
	require.False(t, claimed)
}

func TestBuilderRouteStoreClaimOrAddDoesNotEvictTrustedIdleRoute(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	trustedRoot := common.Hash{1}
	trustedURL := "https://trusted.example"

	require.True(t, routes.Add(trustedRoot, trustedURL))
	_, _, claimed := routes.ClaimOrAdd(common.Hash{2}, "https://untrusted.example")
	require.False(t, claimed)
	_, claimed = routes.Claim(trustedRoot, trustedURL)
	require.True(t, claimed)
}

func TestBuilderRouteStoreClaimOrAddDoesNotEvictCompletedRoute(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	claimID, _, claimed := routes.ClaimOrAdd(common.Hash{1}, "https://one.example")
	require.True(t, claimed)
	routes.Complete(common.Hash{1}, "https://one.example", claimID, true)

	_, _, claimed = routes.ClaimOrAdd(common.Hash{2}, "https://two.example")
	require.False(t, claimed)
}

func TestBuilderRouteStoreCapacityPreservesAcceptedRoutes(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)

	require.True(t, routes.Add(common.Hash{1}, "https://one.example"))
	require.True(t, routes.Add(common.Hash{2}, "https://two.example"))
	require.False(t, routes.Add(common.Hash{3}, "https://three.example"))
	_, claimed := routes.Claim(common.Hash{1}, "https://one.example")
	require.True(t, claimed)
	_, claimed = routes.Claim(common.Hash{2}, "https://two.example")
	require.True(t, claimed)
	_, claimed = routes.Claim(common.Hash{3}, "https://three.example")
	require.False(t, claimed)
}

func TestBuilderRouteStoreExpiryFreesCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	routes := newBuilderRouteStore(1, time.Minute, func() time.Time { return now })

	require.True(t, routes.Add(common.Hash{1}, "https://one.example"))
	require.False(t, routes.Add(common.Hash{2}, "https://two.example"))
	now = now.Add(time.Minute)
	require.True(t, routes.Add(common.Hash{2}, "https://two.example"))
	_, claimed := routes.Claim(common.Hash{1}, "https://one.example")
	require.False(t, claimed)
	_, claimed = routes.Claim(common.Hash{2}, "https://two.example")
	require.True(t, claimed)
}

func TestBuilderRouteStoreEvictsSpentRouteBeforeRejectingPromise(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	firstRoot := common.Hash{1}
	require.True(t, routes.Add(firstRoot, "https://one.example"))
	claimID, claimed := routes.Claim(firstRoot, "https://one.example")
	require.True(t, claimed)
	routes.Complete(firstRoot, "https://one.example", claimID, true)

	require.True(t, routes.Add(common.Hash{2}, "https://two.example"))
	_, claimed = routes.Claim(firstRoot, "https://one.example")
	require.False(t, claimed)
	_, claimed = routes.Claim(common.Hash{2}, "https://two.example")
	require.True(t, claimed)
}

func TestBuilderRouteStoreEvictsOldestSpentRoute(t *testing.T) {
	now := time.Unix(100, 0)
	routes := newBuilderRouteStore(2, time.Minute, func() time.Time { return now })
	first := builderRouteKey{root: common.Hash{1}, url: "https://one.example"}
	second := builderRouteKey{root: common.Hash{2}, url: "https://two.example"}
	require.True(t, routes.Add(first.root, first.url))
	claimID, claimed := routes.Claim(first.root, first.url)
	require.True(t, claimed)
	routes.Complete(first.root, first.url, claimID, true)
	now = now.Add(time.Second)
	require.True(t, routes.Add(second.root, second.url))
	claimID, claimed = routes.Claim(second.root, second.url)
	require.True(t, claimed)
	routes.Complete(second.root, second.url, claimID, true)

	require.True(t, routes.Add(common.Hash{3}, "https://three.example"))
	_, firstExists := routes.routes[first]
	_, secondExists := routes.routes[second]
	require.False(t, firstExists)
	require.True(t, secondExists)
}

func TestBuilderRouteStoreEqualExpiryUsesKeyTieBreak(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint("reverse=", reverse), func(t *testing.T) {
			routes := newBuilderRouteStore(2, time.Minute, func() time.Time { return time.Unix(100, 0) })
			lower := builderRouteKey{root: common.Hash{1}, url: "https://same.example"}
			higher := builderRouteKey{root: common.Hash{2}, url: "https://same.example"}
			keys := []builderRouteKey{lower, higher}
			if reverse {
				keys[0], keys[1] = keys[1], keys[0]
			}
			for _, key := range keys {
				require.True(t, routes.Add(key.root, key.url))
				claimID, claimed := routes.Claim(key.root, key.url)
				require.True(t, claimed)
				routes.Complete(key.root, key.url, claimID, true)
			}

			require.True(t, routes.Add(common.Hash{3}, "https://three.example"))
			_, lowerExists := routes.routes[lower]
			_, higherExists := routes.routes[higher]
			require.False(t, lowerExists)
			require.True(t, higherExists)
		})
	}
}
