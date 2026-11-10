package eth

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0o750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := range 3 {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0o750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := range 2 {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0o750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := range 2 {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0o750)
				require.NoError(t, err)
				//t.Log("OK")
			}
		}
	}
	list, err := os.ReadDir(rootName)
	require.NoError(t, err)

	require.Len(t, list, 3)

	err = RemoveContents(rootName)
	require.NoError(t, err)

	list, err = os.ReadDir(rootName)
	require.NoError(t, err)

	require.Empty(t, list)
}

// MCP serves only the namespaces --http.api enables: it must not force admin, debug
// or trace on, and an empty list gives it no namespace-backed RPC methods.
func TestMCPNamespacesFollowHTTPAPI(t *testing.T) {
	for _, tc := range []struct {
		name string
		api  []string
		want []string
	}{
		{"eth only", []string{"eth"}, []string{"eth"}},
		{"debug enabled", []string{"eth", "debug"}, []string{"debug", "eth"}},
		{"empty", nil, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := httpcfg.HttpCfg{API: tc.api}
			logger := log.New()
			apis := jsonrpc.APIList(nil, nil, nil, nil, nil, nil, freezeblocks.NewBlockReader(nil), &cfg, nil, logger, nil, nil)
			srv, err := newMCPRPCServer(apis, &cfg, logger)
			require.NoError(t, err)
			defer srv.Stop()
			client := rpc.DialInProc(srv, logger)
			defer client.Close()

			modules, err := client.SupportedModules()
			require.NoError(t, err)
			got := slices.Sorted(maps.Keys(modules))
			got = slices.DeleteFunc(got, func(ns string) bool { return ns == "rpc" })
			require.Equal(t, tc.want, got)
		})
	}
}

// TestBackwardBlockDownloader: the node exposes its downloader of blocks by
// hash, which draws on the node's peers: with none it asks no one.
func TestBackwardBlockDownloader(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	stack, err := node.New(t.Context(), &nodecfg.Config{
		Dirs: dirs,
		P2P: p2p.Config{
			ListenAddr:      "127.0.0.1:0",
			MaxPeers:        1,
			MaxPendingPeers: 1,
			NoDiscovery:     true,
			NoDial:          true,
			ProtocolVersion: []uint{direct.ETH69},
			PrivateKey:      key,
		},
		MdbxDBSizeLimit: datasize.GB,
	}, logger)
	require.NoError(t, err)
	cfg := ethconfig.Defaults
	cfg.Dirs = dirs
	cfg.Genesis = chainspec.DeveloperGenesisBlock()
	cfg.Snapshot.NoDownloader = true
	cfg.TxPool.Disable = true
	cfg.InternalCL = false
	cfg.StateCacheBudget = datasize.MB
	backend, err := New(t.Context(), stack, &cfg, logger, nil)
	require.NoError(t, err)
	require.NoError(t, backend.Init(stack, &cfg, backend.ChainConfig()))
	require.NoError(t, stack.Start())
	t.Cleanup(func() { require.NoError(t, stack.Close()) })

	bbd := backend.BackwardBlockDownloader()
	require.NotNil(t, bbd)
	feed, err := bbd.DownloadBlocksBackwards(t.Context(), common.Hash{1}, nil)
	require.NoError(t, err)
	_, err = feed.Next(t.Context())
	require.ErrorContains(t, err, "no peers available")
}

// TestSetHeadRefusal: SetHead moves the head through the execution module,
// unless the node refuses head moves on request, which it does with the
// reason before reaching the module.
func TestSetHeadRefusal(t *testing.T) {
	m := execmoduletester.New(t)
	s := &Ethereum{config: &ethconfig.Config{}, execModule: m.ExecModule}
	require.NoError(t, s.SetHead(t.Context(), 0))
	require.ErrorContains(t, s.SetHead(t.Context(), 1), "future block", "the execution module checks the target")

	refusing := &Ethereum{config: &ethconfig.Config{SetHeadRefusal: "another component drives the chain"}}
	err := refusing.SetHead(t.Context(), 0)
	require.ErrorIs(t, err, ErrSetHeadRefused)
	require.ErrorContains(t, err, "another component drives the chain")
}
