package jsonrpc

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/mocks"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"github.com/ledgerwatch/erigon/zkevm/state"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	host                      = "localhost"
	maxRequestsPerIPAndSecond = 1000
)

type mockedServer struct {
	Config    Config
	Server    *Server
	ServerURL string
}

type mocksWrapper struct {
	Pool    *mocks.PoolMock
	State   *mocks.StateMock
	Storage *storageMock
	DbTx    *mocks.DBTxMock
}

func newMockedServer(t *testing.T, cfg Config) (*mockedServer, *mocksWrapper, *ethclient.Client) {
	pool := mocks.NewPoolMock(t)
	st := mocks.NewStateMock(t)
	storage := newStorageMock(t)
	dbTx := mocks.NewDBTxMock(t)
	apis := map[string]bool{
		APIEth:    true,
		APINet:    true,
		APIDebug:  true,
		APIZKEVM:  true,
		APITxPool: true,
		APIWeb3:   true,
	}

	var newL2BlockEventHandler state.NewL2BlockEventHandler = func(e state.NewL2BlockEvent) {}
	st.On("RegisterNewL2BlockEventHandler", mock.IsType(newL2BlockEventHandler)).Once()

	st.On("PrepareWebSocket").Once()
	server := NewServer(cfg, pool, st, storage, apis)

	go func() {
		err := server.Start()
		if err != nil {
			panic(err)
		}
	}()

	serverURL := fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
	for {
		fmt.Println("waiting server to get ready...") // fmt is used here to avoid race condition with logs
		res, err := http.Get(serverURL)               //nolint:gosec
		if err == nil && res.StatusCode == http.StatusOK {
			fmt.Println("server ready!") // fmt is used here to avoid race condition with logs
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	ethClient, err := ethclient.Dial(serverURL)
	require.NoError(t, err)

	msv := &mockedServer{
		Config:    cfg,
		Server:    server,
		ServerURL: serverURL,
	}

	mks := &mocksWrapper{
		Pool:    pool,
		State:   st,
		Storage: storage,
		DbTx:    dbTx,
	}

	return msv, mks, ethClient
}

func getDefaultConfig() Config {
	cfg := Config{
		Host:                      host,
		Port:                      9123,
		MaxRequestsPerIPAndSecond: maxRequestsPerIPAndSecond,
		DefaultSenderAddress:      "0x1111111111111111111111111111111111111111",
		MaxCumulativeGasUsed:      300000,
		ChainID:                   1000,
	}
	return cfg
}

func newSequencerMockedServer(t *testing.T) (*mockedServer, *mocksWrapper, *ethclient.Client) {
	cfg := getDefaultConfig()
	return newMockedServer(t, cfg)
}

func newNonSequencerMockedServer(t *testing.T, sequencerNodeURI string) (*mockedServer, *mocksWrapper, *ethclient.Client) {
	cfg := getDefaultConfig()
	cfg.Port = 9124
	cfg.SequencerNodeURI = sequencerNodeURI
	return newMockedServer(t, cfg)
}

func (s *mockedServer) Stop() {
	err := s.Server.Stop()
	if err != nil {
		panic(err)
	}
}

func (s *mockedServer) JSONRPCCall(method string, parameters ...interface{}) (types.Response, error) {
	return client.JSONRPCCall(s.ServerURL, method, parameters...)
}
