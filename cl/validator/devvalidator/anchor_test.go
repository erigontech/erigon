package devvalidator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
)

func headSlotServer(t *testing.T, slot string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/eth/v1/beacon/headers/head", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{"data": map[string]interface{}{
			"header": map[string]interface{}{"message": map[string]interface{}{"slot": slot}},
		}}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	})
	return httptest.NewServer(mux)
}

// A slow/loaded boot stamps genesis_time long before the producer starts, so the wall-clock
// slot runs far past a chain still at genesis. anchorClockToHead must pull the clock back to the
// head so the producer resumes at head+1 instead of jumping to a future epoch (which 404s
// proposer duties and deadlocks the chain at block 0).
func TestAnchorClockToHead_ReanchorsWhenWallClockAhead(t *testing.T) {
	srv := headSlotServer(t, "0")
	defer srv.Close()

	cfg := &clparams.BeaconChainConfig{SecondsPerSlot: 2}
	s := &Service{
		client:      NewBeaconClient(srv.URL),
		cfg:         cfg,
		genesisTime: uint64(time.Now().Unix()) - 200, // 100 slots of drift
		logger:      log.New(),
	}

	s.anchorClockToHead(context.Background())

	wallSlot := (uint64(time.Now().Unix()) - s.genesisTime) / cfg.SecondsPerSlot
	require.LessOrEqual(t, wallSlot, uint64(1), "clock must be re-anchored to head slot 0")
}

// A fast boot (head already tracking wall-clock) must be left untouched, so the chain keeps its
// real-time slot cadence.
func TestAnchorClockToHead_NoopWhenCaughtUp(t *testing.T) {
	srv := headSlotServer(t, "1")
	defer srv.Close()

	cfg := &clparams.BeaconChainConfig{SecondsPerSlot: 2}
	original := uint64(time.Now().Unix()) - 2 // wall-clock slot 1, head slot 1 -> no re-anchor
	s := &Service{
		client:      NewBeaconClient(srv.URL),
		cfg:         cfg,
		genesisTime: original,
		logger:      log.New(),
	}

	s.anchorClockToHead(context.Background())

	require.Equal(t, original, s.genesisTime, "caught-up clock must not be re-anchored")
}
