package checkpoint_sync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/datadir"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// newMockHttpServer creates a mock HTTP server that encodes and returns the expected state
func newMockHttpServer(expectedState *state.CachingBeaconState, sent *bool) *httptest.Server {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enc, err := expectedState.EncodeSSZ(nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("could not encode state: %s", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, err = w.Write(enc)
		if err != nil {
			http.Error(w, fmt.Sprintf("could not write encoded state: %s", err), http.StatusInternalServerError)
			return
		}
		*sent = true
	}))
	return mockServer
}

// newMockSlowHttpServer creates a mock HTTP server that never responds and exits gracefully when context is cancelled
func newMockSlowHttpServer(ctx context.Context) *httptest.Server {
	mockSlowServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}))
	return mockSlowServer
}

func TestRemoteCheckpointSync(t *testing.T) {
	// Create a mock HTTP server always returning the passed expected state
	_, expectedState, _ := tests.GetPhase0Random()
	rec := false
	mockServer := newMockHttpServer(expectedState, &rec)
	defer mockServer.Close()

	// Only 1 OK HTTP server, so we must get the expected state
	clparams.ConfigurableCheckpointsURLs = []string{mockServer.URL}
	syncer := NewRemoteCheckpointSync(&clparams.MainnetBeaconConfig, chainspec.MainnetChainID)
	actualState, err := syncer.GetLatestBeaconState(context.Background())
	assert.True(t, rec)
	require.NoError(t, err)
	require.NotNil(t, actualState)

	// Compare the roots of the states
	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	actualRoot, err := actualState.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedRoot, actualRoot)
}

func TestRemoteCheckpointSyncTimeout(t *testing.T) {
	// Create a mock for very slow HTTP server
	ctx, cancel := context.WithCancel(context.Background())
	mockSlowServer := newMockSlowHttpServer(ctx)
	defer mockSlowServer.Close()
	defer cancel()

	// Only slow HTTP servers, so we must get a timeout
	clparams.ConfigurableCheckpointsURLs = []string{mockSlowServer.URL, mockSlowServer.URL, mockSlowServer.URL}
	syncer := &RemoteCheckpointSync{&clparams.MainnetBeaconConfig, chainspec.MainnetChainID, 50 * time.Millisecond}
	currentState, err := syncer.GetLatestBeaconState(ctx)
	require.Nil(t, currentState)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestRemoteCheckpointSyncCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	clparams.ConfigurableCheckpointsURLs = []string{"http://127.0.0.1:1"}
	syncer := NewRemoteCheckpointSync(&clparams.MainnetBeaconConfig, chainspec.MainnetChainID)
	currentState, err := syncer.GetLatestBeaconState(ctx)

	require.Nil(t, currentState)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRemoteCheckpointSyncPossiblyAfterTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Create a mock for very slow HTTP server
	ctx, cancel := context.WithCancel(context.Background())
	mockSlowServer := newMockSlowHttpServer(ctx)
	defer mockSlowServer.Close()
	defer cancel()

	// Create a mock HTTP server always returning the passed expected state
	_, expectedState, _ := tests.GetPhase0Random()
	rec := false
	mockServer := newMockHttpServer(expectedState, &rec)
	defer mockServer.Close()

	// 3 slow + 1 OK HTTP servers, so we may get some timeout(s) with probability 0.75 but will eventually succeed
	clparams.ConfigurableCheckpointsURLs = []string{mockSlowServer.URL, mockSlowServer.URL, mockSlowServer.URL, mockServer.URL}
	syncer := &RemoteCheckpointSync{&clparams.MainnetBeaconConfig, chainspec.MainnetChainID, 1 * time.Second}
	actualState, err := syncer.GetLatestBeaconState(ctx)
	assert.True(t, rec)
	require.NoError(t, err)
	require.NotNil(t, actualState)

	// Compare the roots of the states
	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	actualRoot, err := actualState.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedRoot, actualRoot)
}

func TestNormalizeCheckpointURL(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"https://checkpoint-sync.example.io", "https://checkpoint-sync.example.io/eth/v2/debug/beacon/states/finalized"},
		{"https://checkpoint-sync.example.io/", "https://checkpoint-sync.example.io/eth/v2/debug/beacon/states/finalized"},
		{"https://checkpoint-sync.example.io/eth/v2/debug/beacon/states/finalized", "https://checkpoint-sync.example.io/eth/v2/debug/beacon/states/finalized"},
		{"https://example.io/eth/v2/debug/beacon/states/head", "https://example.io/eth/v2/debug/beacon/states/head"},
	}
	for _, tt := range tests {
		got := normalizeCheckpointURL(tt.input)
		assert.Equal(t, tt.want, got, "normalizeCheckpointURL(%q)", tt.input)
	}
}

func TestRemoteCheckpointSyncRejectsHTML(t *testing.T) {
	mockHTMLServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<!doctype html><html><head><meta charset="utf-8"/></head></html>`)
	}))
	defer mockHTMLServer.Close()

	clparams.ConfigurableCheckpointsURLs = []string{mockHTMLServer.URL + beaconStatePath}
	syncer := NewRemoteCheckpointSync(&clparams.MainnetBeaconConfig, chainspec.MainnetChainID)
	st, err := syncer.GetLatestBeaconState(context.Background())
	require.Nil(t, st)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected content-type")
}

func TestLocalCheckpointSyncFromFile(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	f := afero.NewMemMapFs()
	enc, err := st.EncodeSSZ(nil)
	enc = utils.CompressSnappy(enc)
	require.NoError(t, err)
	require.NoError(t, afero.WriteFile(f, clparams.LatestStateFileName, enc, 0o644))

	genesisState, err := st.Copy()
	require.NoError(t, err)
	genesisState.AddEth1DataVote(cltypes.NewEth1Data()) // Add some data to the genesis state so that it is different from the state read from the file

	syncer := NewLocalCheckpointSyncer(genesisState, f)
	state, err := syncer.GetLatestBeaconState(context.Background())
	require.NoError(t, err)
	require.NotNil(t, state)
	// Compare the roots of the states
	haveRoot, err := st.HashSSZ()
	require.NoError(t, err)
	wantRoot, err := state.HashSSZ()
	require.NoError(t, err)

	assert.Equal(t, wantRoot, haveRoot)
}

func TestReadLocalFinalizedState_RoundTrip(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	dirs := datadir.New(t.TempDir())

	enc, err := st.EncodeSSZ(nil)
	require.NoError(t, err)
	enc = utils.CompressSnappy(enc)
	statePath := filepath.Join(dirs.CaplinLatest, clparams.LatestFinalizedStateFileName)
	require.NoError(t, os.WriteFile(statePath, enc, 0o644))

	got, err := ReadLocalFinalizedState(dirs, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	require.NotNil(t, got)

	wantRoot, err := st.HashSSZ()
	require.NoError(t, err)
	gotRoot, err := got.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, wantRoot, gotRoot)
}

func TestReadLocalFinalizedState_Absent(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	got, err := ReadLocalFinalizedState(dirs, &clparams.MainnetBeaconConfig)
	require.Error(t, err)
	require.Nil(t, got)
}

func TestStateWithinResumeHorizon(t *testing.T) {
	const (
		genesisTime    = uint64(1_000_000)
		secondsPerSlot = uint64(12)
		horizonSlots   = uint64(100)
	)
	// slotToNow returns a now-unix at which the current slot equals wantCurrentSlot.
	slotToNow := func(wantCurrentSlot uint64) uint64 {
		return genesisTime + wantCurrentSlot*secondsPerSlot
	}

	tests := []struct {
		name           string
		localSlot      uint64
		nowUnix        uint64
		secondsPerSlot uint64
		want           bool
	}{
		{"equal", 500, slotToNow(500), secondsPerSlot, true},
		{"one-behind", 499, slotToNow(500), secondsPerSlot, true},
		{"exactly-at-horizon", 400, slotToNow(500), secondsPerSlot, true},
		{"just-beyond", 399, slotToNow(500), secondsPerSlot, false},
		{"far-beyond", 100, slotToNow(500), secondsPerSlot, false},
		{"local-ahead", 600, slotToNow(500), secondsPerSlot, true},
		{"now-before-genesis", 500, genesisTime - 1, secondsPerSlot, true},
		{"zero-seconds-per-slot", 100, slotToNow(500), 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stateWithinResumeHorizon(tt.localSlot, genesisTime, tt.nowUnix, tt.secondsPerSlot, horizonSlots)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLocalCheckpointSyncFromGenesis(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	f := afero.NewMemMapFs()

	syncer := NewLocalCheckpointSyncer(st, f)
	state, err := syncer.GetLatestBeaconState(context.Background())
	require.NoError(t, err)
	require.NotNil(t, state)
	// Compare the roots of the states
	haveRoot, err := st.HashSSZ()
	require.NoError(t, err)
	wantRoot, err := state.HashSSZ()
	require.NoError(t, err)

	assert.Equal(t, wantRoot, haveRoot)
}
