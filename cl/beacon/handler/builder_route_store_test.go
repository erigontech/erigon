// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
// SPDX-License-Identifier: LGPL-3.0-or-later

package handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/common"
)

func TestBuilderRouteStoreReaddingDeliveredRouteDoesNotRearm(t *testing.T) {
	now := time.Unix(100, 0)
	routes := newBuilderRouteStore(2, time.Minute, func() time.Time { return now })
	root := common.Hash{1}
	url := "https://builder.example"

	require.True(t, routes.Add(root, url))
	require.True(t, routes.Claim(root, url))
	routes.Complete(root, url, true)
	require.True(t, routes.Add(root, url))
	require.False(t, routes.Claim(root, url))
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

func TestBuilderRouteStoreAllowsAliasesForSameRoot(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)
	root := common.Hash{1}

	require.True(t, routes.Add(root, "https://one.example"))
	require.True(t, routes.Add(root, "https://two.example"))
	require.True(t, routes.Claim(root, "https://one.example"))
	require.True(t, routes.Claim(root, "https://two.example"))
}

func TestBuilderRouteStoreCapacityPreservesAcceptedRoutes(t *testing.T) {
	routes := newBuilderRouteStore(2, time.Minute, time.Now)

	require.True(t, routes.Add(common.Hash{1}, "https://one.example"))
	require.True(t, routes.Add(common.Hash{2}, "https://two.example"))
	require.False(t, routes.Add(common.Hash{3}, "https://three.example"))
	require.True(t, routes.Claim(common.Hash{1}, "https://one.example"))
	require.True(t, routes.Claim(common.Hash{2}, "https://two.example"))
	require.False(t, routes.Claim(common.Hash{3}, "https://three.example"))
}

func TestBuilderRouteStoreExpiryFreesCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	routes := newBuilderRouteStore(1, time.Minute, func() time.Time { return now })

	require.True(t, routes.Add(common.Hash{1}, "https://one.example"))
	require.False(t, routes.Add(common.Hash{2}, "https://two.example"))
	now = now.Add(time.Minute)
	require.True(t, routes.Add(common.Hash{2}, "https://two.example"))
	require.False(t, routes.Claim(common.Hash{1}, "https://one.example"))
	require.True(t, routes.Claim(common.Hash{2}, "https://two.example"))
}

func TestBuilderRouteStoreEvictsDeliveredRouteBeforeRejectingPromise(t *testing.T) {
	routes := newBuilderRouteStore(1, time.Minute, time.Now)
	firstRoot := common.Hash{1}
	require.True(t, routes.Add(firstRoot, "https://one.example"))
	require.True(t, routes.Claim(firstRoot, "https://one.example"))
	routes.Complete(firstRoot, "https://one.example", true)

	require.True(t, routes.Add(common.Hash{2}, "https://two.example"))
	require.False(t, routes.Claim(firstRoot, "https://one.example"))
	require.True(t, routes.Claim(common.Hash{2}, "https://two.example"))
}
