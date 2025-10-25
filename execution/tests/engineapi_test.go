package executiontests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/jwt"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestEngineApiTestJsonUnmarshall(t *testing.T) {
	var engineApiTest EngineApiTest
	bytes, err := os.ReadFile(filepath.Join(".", "engineapi-performance-tests", "EcAdd12CACHABLE_150M.json"))
	require.NoError(t, err)
	err = json.Unmarshal(bytes, &engineApiTest)
	require.NoError(t, err)
}

func TestEngineApiPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	if runtime.GOOS == "windows" {
		// this test fails on windows ci with the following error:
		// panic: fail to open mdbx: mdbx_env_open: The paging file is too small for this operation to complete.
		t.Skip("causes mdbx 'paging file is too small' panic")
	}

	engineApiTestDir := filepath.Join(".", "engineapi-performance-tests")
	tm := new(testMatcher)
	tm.walk(t, engineApiTestDir, func(t *testing.T, name string, test *EngineApiTest) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := tm.checkFailure(t, test.Run(ctx, t)); err != nil {
			t.Error(err)
		}
	})
}

type EngineApiTest struct {
	Genesis  *types.Genesis      `json:"genesis"`
	Requests []*EngineApiRequest `json:"requests"`
}

func (eat *EngineApiTest) Run(ctx context.Context, t *testing.T) error {
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	engineApiPort, err := testutil.NextFreePort()
	require.NoError(t, err)

	httpConfig := httpcfg.HttpCfg{
		AuthRpcHTTPListenAddress: "127.0.0.1",
		AuthRpcPort:              engineApiPort,
		JWTSecretPath:            path.Join(dataDir, "jwt.hex"),
	}

	nodeKeyConfig := p2p.NodeKeyConfig{}
	nodeKey, err := nodeKeyConfig.LoadOrGenerateAndSave(nodeKeyConfig.DefaultPath(dataDir))
	require.NoError(t, err)
	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P: p2p.Config{
			ListenAddr:      fmt.Sprintf("127.0.0.1:0"),
			MaxPeers:        1,
			MaxPendingPeers: 1,
			NoDiscovery:     true,
			NoDial:          true,
			ProtocolVersion: []uint{},
			AllowedPorts:    []uint{},
			PrivateKey:      nodeKey,
		},
	}

	txPoolConfig := txpoolcfg.DefaultConfig
	txPoolConfig.DBDir = dirs.TxPool

	ethConfig := ethconfig.Config{
		Dirs: dirs,
		Snapshot: ethconfig.BlocksFreezing{
			NoDownloader: true,
		},
		TxPool: txPoolConfig,
		Miner: buildercfg.MiningConfig{
			EnabledPOS: true,
		},
		ErigonDBStepSize:          config3.DefaultStepSize,
		ErigonDBStepsInFrozenFile: config3.DefaultStepsInFrozenFile,
		FcuTimeout:                10 * time.Second, // CI tests can run slowly
	}

	ethNode, err := node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)
	cleanNode := func(ethNode *node.Node) func() {
		return func() {
			err := ethNode.Close()
			if errors.Is(err, node.ErrNodeStopped) {
				return
			}
			require.NoError(t, err)
		}
	}
	t.Cleanup(cleanNode(ethNode))

	// init genesis
	chainDB, err := node.OpenDatabase(ctx, ethNode.Config(), dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	_, _, err = genesiswrite.CommitGenesisBlock(chainDB, eat.Genesis, ethNode.Config().Dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	// start node
	ethBackend, err := eth.New(ctx, ethNode, &ethConfig, logger, nil)
	require.NoError(t, err)
	err = ethBackend.Init(ethNode, &ethConfig, eat.Genesis.Config)
	require.NoError(t, err)
	err = ethNode.Start()
	require.NoError(t, err)

	// init engine api client
	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	require.NoError(t, err)
	//goland:noinspection HttpUrlsUsage
	engineApiUrl := fmt.Sprintf("http://%s:%d", httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	jwtRoundTripper := jwt.NewHttpRoundTripper(http.DefaultTransport, jwtSecret)
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: jwtRoundTripper}
	rpcClient, err := rpc.DialHTTPWithClient(engineApiUrl, httpClient, logger)
	require.NoError(t, err)

	// perform requests
	for _, request := range eat.Requests {
		if strings.HasPrefix(request.Method, "engine_forkchoiceUpdatedV") {
			var result enginetypes.ForkChoiceUpdatedResponse
			err = rpcClient.CallContext(ctx, &result, request.Method, request.Params...)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, result.PayloadStatus.Status)
		} else if strings.HasPrefix(request.Method, "engine_newPayloadV") {
			var result enginetypes.PayloadStatus
			err = rpcClient.CallContext(ctx, &result, request.Method, request.Params...)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, result.Status)
		} else {
			panic("unexpected method: " + request.Method)
		}
	}

	return nil
}

type EngineApiRequest struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
	Id     uint64        `json:"id"`
}
