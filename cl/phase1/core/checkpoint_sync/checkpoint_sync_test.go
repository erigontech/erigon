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
