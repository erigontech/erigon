package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/jwt"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon-lib/types"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/tests/testports"
	enginetypes "github.com/erigontech/erigon/turbo/engineapi/engine_types"
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

	engineApiTestDir := filepath.Join(".", "engineapi-performance-tests")
	tm := new(testMatcher)
	tm.walk(t, engineApiTestDir, func(t *testing.T, name string, test *EngineApiTest) {
		t.Parallel()

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
	engineApiPort, err := testports.NextFreePort()
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
		Miner: params.MiningConfig{
			EnabledPOS: true,
		},
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
	chainDB, err := node.OpenDatabase(ctx, ethNode.Config(), kv.ChainDB, "", false, logger)
	require.NoError(t, err)
	_, _, err = core.CommitGenesisBlock(chainDB, eat.Genesis, ethNode.Config().Dirs, logger)
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
	sendRequestWithRetry := func(req func() error) error {
		// needed as there may be a race between when engine api has been initialised and this test hitting it
		return backoff.Retry(req, backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 100))
	}
	timeout := time.After(time.Minute)
	queue := eat.Requests
	requestCounter := 1
	for len(queue) > 0 {
		request := queue[0]
		var status enginetypes.EngineStatus
		var requestType string
		if strings.HasPrefix(request.Method, "engine_forkchoiceUpdatedV") {
			var result enginetypes.ForkChoiceUpdatedResponse
			err = sendRequestWithRetry(func() error {
				return rpcClient.CallContext(ctx, &result, request.Method, request.Params...)
			})
			require.NoError(t, err)
			status = result.PayloadStatus.Status
			requestType = "forkchoiceUpdated"
		} else if strings.HasPrefix(request.Method, "engine_newPayloadV") {
			var result enginetypes.PayloadStatus
			err = sendRequestWithRetry(func() error {
				return rpcClient.CallContext(ctx, &result, request.Method, request.Params...)
			})
			require.NoError(t, err)
			status = result.Status
			requestType = "newPayload"
		} else {
			panic("unexpected method: " + request.Method)
		}

		if status == enginetypes.SyncingStatus {
			select {
			case <-timeout:
				t.Fatalf("timeout waiting for syncing to finish: request=%d, type=%s", requestCounter, requestType)
			default: // continue
			}

			logger.Debug("waiting for syncing to finish", "request", requestCounter, "type", requestType)
			time.Sleep(200 * time.Millisecond)
			continue // retry the request
		}

		require.Equal(t, enginetypes.ValidStatus, status, "unexpected status for request=%d, type=%s", requestCounter, requestType)
		queue = queue[1:]
		requestCounter++
	}

	return nil
}

type EngineApiRequest struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
	Id     uint64        `json:"id"`
}
