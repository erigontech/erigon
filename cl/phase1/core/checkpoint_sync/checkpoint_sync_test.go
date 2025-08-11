package checkpoint_sync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestRemoteCheckpointSync(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	rec := false
	// Create a mock HTTP server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enc, err := st.EncodeSSZ(nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("could not encode state: %s", err), http.StatusInternalServerError)
			return
		}
		w.Write(enc)
		rec = true
	}))
	defer mockServer.Close()

	clparams.ConfigurableCheckpointsURLs = []string{mockServer.URL}
	syncer := NewRemoteCheckpointSync(&clparams.MainnetBeaconConfig, chainspec.MainnetChainID)
	state, err := syncer.GetLatestBeaconState(context.Background())
	assert.True(t, rec)
	require.NoError(t, err)
	require.NotNil(t, state)
	// Compare the roots of the states
	haveRoot, err := st.HashSSZ()
	require.NoError(t, err)
	wantRoot, err := state.HashSSZ()
	require.NoError(t, err)

	assert.Equal(t, wantRoot, haveRoot)
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
