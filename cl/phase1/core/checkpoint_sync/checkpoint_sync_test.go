package checkpoint_sync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func TestRemoteCheckpointSyncHeadExecutionBlockNumber(t *testing.T) {
	const blockNumber = uint64(20922936)
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/eth/v2/beacon/blocks/head", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, err := fmt.Fprintf(w, `{"data":{"message":{"body":{"execution_payload":{"block_number":"%d"}}}}}`, blockNumber)
		require.NoError(t, err)
	}))
	defer mockServer.Close()

	prevURLs := clparams.ConfigurableCheckpointsURLs
	t.Cleanup(func() { clparams.ConfigurableCheckpointsURLs = prevURLs })
	clparams.ConfigurableCheckpointsURLs = []string{mockServer.URL + "/eth/v2/debug/beacon/states/finalized"}

	syncer := &RemoteCheckpointSync{
		beaconConfig: &clparams.MainnetBeaconConfig,
		net:          chainspec.MainnetChainID,
		timeout:      time.Second,
	}
	actualBlockNumber, ok, err := syncer.GetHeadExecutionBlockNumber(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, blockNumber, actualBlockNumber)
}

func TestHeadBlockURI(t *testing.T) {
	actual, err := headBlockURI("https://checkpoint.example.com/prefix/eth/v2/debug/beacon/states/finalized?ignored=true")
	require.NoError(t, err)
	require.Equal(t, "https://checkpoint.example.com/prefix/eth/v2/beacon/blocks/head", actual)
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
	require.NoError(t, afero.WriteFile(f, clparams.LatestStateFileName, enc, 0644))

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
