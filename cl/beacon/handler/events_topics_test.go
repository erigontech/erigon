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
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestEventStreamRepeatedTopicsParam(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
	server := httptest.NewServer(http.HandlerFunc(a.EventSourceGetV1Events))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			if a.emitters.State().SendBlock(&beaconevents.BlockData{Slot: 7}) > 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/eth/v1/events?topics=head&topics=block", nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "event: ") {
			require.Equal(t, "event: block", scanner.Text())
			return
		}
	}
	t.Fatal("block event not delivered for the second topics parameter")
}

func TestEventStreamRequiresTopics(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
	rec := httptest.NewRecorder()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	a.EventSourceGetV1Events(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/eth/v1/events", nil))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
