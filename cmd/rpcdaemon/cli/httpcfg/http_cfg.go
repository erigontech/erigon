package httpcfg

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
)

type HttpCfg struct {
	Enabled bool

	GraphQLEnabled           bool
	WithDatadir              bool // Erigon's database can be read by separated processes on same machine - in read-only mode - with full support of transactions. It will share same "OS PageCache" with Erigon process.
	DataDir                  string
	Dirs                     datadir.Dirs
	AuthRpcHTTPListenAddress string
	TLSCertfile              string
	TLSCACert                string
	TLSKeyFile               string

	HttpServerEnabled  bool
	HttpURL            string
	HttpListenAddress  string
	HttpPort           int
	HttpCORSDomain     []string
	HttpVirtualHost    []string
	AuthRpcVirtualHost []string
	HttpCompression    bool

	HttpsServerEnabled bool
	HttpsURL           string
	HttpsListenAddress string
	HttpsPort          int
	HttpsCertfile      string
	HttpsKeyFile       string

	AuthRpcPort    int
	PrivateApiAddr string

	API                               []string
	Gascap                            uint64
	MaxTraces                         uint64
	WebsocketPort                     int
	WebsocketEnabled                  bool
	WebsocketCompression              bool
	WebsocketSubscribeLogsChannelSize int
	RpcAllowListFilePath              string
	RpcBatchConcurrency               uint
	RpcStreamingDisable               bool
	DBReadConcurrency                 int
	TraceCompatibility                bool // Bug for bug compatibility for trace_ routines with OpenEthereum
	TxPoolApiAddr                     string
	StateCache                        kvcache.CoherentConfig
	Snap                              ethconfig.BlocksFreezing
	Sync                              ethconfig.Sync

	// GRPC server
	GRPCServerEnabled      bool
	GRPCListenAddress      string
	GRPCPort               int
	GRPCHealthCheckEnabled bool

	// Socket Server
	SocketServerEnabled bool
	SocketListenUrl     string

	JWTSecretPath             string // Engine API Authentication
	TraceRequests             bool   // Print requests to logs at INFO level
	DebugSingleRequest        bool   // Print single-request-related debugging info to logs at INFO level
	HTTPTimeouts              rpccfg.HTTPTimeouts
	AuthRpcTimeouts           rpccfg.HTTPTimeouts
	EvmCallTimeout            time.Duration
	OverlayGetLogsTimeout     time.Duration
	OverlayReplayBlockTimeout time.Duration

	LogDirVerbosity string
	LogDirPath      string

	BatchLimit                  int  // Maximum number of requests in a batch
	ReturnDataLimit             int  // Maximum number of bytes returned from calls (like eth_call)
	AllowUnprotectedTxs         bool // Whether to allow non EIP-155 protected transactions  txs over RPC
	MaxGetProofRewindBlockCount int  //Max GetProof rewind block count
	// Ots API
	OtsMaxPageSize uint64

	RPCSlowLogThreshold time.Duration
}
