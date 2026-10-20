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
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestEventStreamSubscribesToEveryTopicsValue(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"topics=head&topics=block", []string{"block", "head"}},
		{"topics=head,block", []string{"block", "head"}},
		{"topics=head,block&topics=finalized_checkpoint", []string{"block", "finalized_checkpoint", "head"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.SecondsPerSlot = 1
			a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
			server := httptest.NewServer(http.HandlerFunc(a.EventSourceGetV1Events))
			defer server.Close()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			go func() {
				for ctx.Err() == nil {
					a.emitters.State().SendHead(&beaconevents.HeadData{Slot: 7})
					a.emitters.State().SendBlock(&beaconevents.BlockData{Slot: 7})
					a.emitters.State().SendFinalizedCheckpoint(&beaconevents.FinalizedCheckpointData{Epoch: 1})
					a.emitters.State().SendChainReorg(&beaconevents.ChainReorgData{Slot: 7})
					time.Sleep(10 * time.Millisecond)
				}
			}()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/eth/v1/events?"+tc.query, nil)
			require.NoError(t, err)
			resp, err := server.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

			seen := map[string]int{}
			scanner := bufio.NewScanner(resp.Body)
			for slices.ContainsFunc(tc.want, func(topic string) bool { return seen[topic] < 2 }) && scanner.Scan() {
				if topic, ok := strings.CutPrefix(scanner.Text(), "event: "); ok {
					require.Contains(t, tc.want, topic)
					seen[topic]++
				}
			}
			require.Equal(t, tc.want, slices.Sorted(maps.Keys(seen)))
		})
	}
}

func TestEventStreamRejectsMissingOrInvalidTopics(t *testing.T) {
	for _, query := range []string{"", "?topics=bogus", "?topics=head&topics=bogus", "?topics=head&topics="} {
		t.Run(query, func(t *testing.T) {
			rec := httptest.NewRecorder()
			(&ApiHandler{}).EventSourceGetV1Events(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/events"+query, nil))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		})
	}
}
