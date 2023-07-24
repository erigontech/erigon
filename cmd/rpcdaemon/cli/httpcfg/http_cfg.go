package httpcfg

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
)

type HttpCfg struct {
	Enabled                  bool
	PrivateApiAddr           string
	GraphQLEnabled           bool
	WithDatadir              bool // Erigon's database can be read by separated processes on same machine - in read-only mode - with full support of transactions. It will share same "OS PageCache" with Erigon process.
	DataDir                  string
	Dirs                     datadir.Dirs
	HttpListenAddress        string
	AuthRpcHTTPListenAddress string
	TLSCertfile              string
	TLSCACert                string
	TLSKeyFile               string
	HttpPort                 int
	AuthRpcPort              int
	HttpCORSDomain           []string
	HttpVirtualHost          []string
	AuthRpcVirtualHost       []string
	HttpCompression          bool
	API                      []string
	Gascap                   uint64
	MaxTraces                uint64
	WebsocketEnabled         bool
	WebsocketCompression     bool
	RpcAllowListFilePath     string
	RpcBatchConcurrency      uint
	RpcStreamingDisable      bool
	DBReadConcurrency        int
	TraceCompatibility       bool // Bug for bug compatibility for trace_ routines with OpenEthereum
	TxPoolApiAddr            string
	StateCache               kvcache.CoherentConfig
	Snap                     ethconfig.BlocksFreezing
	Sync                     ethconfig.Sync

	// GRPC server
	GRPCServerEnabled      bool
	GRPCListenAddress      string
	GRPCPort               int
	GRPCHealthCheckEnabled bool

	// Raw TCP Server
	TCPServerEnabled bool
	TCPListenAddress string
	TCPPort          int

	JWTSecretPath   string // Engine API Authentication
	TraceRequests   bool   // Always trace requests in INFO level
	HTTPTimeouts    rpccfg.HTTPTimeouts
	AuthRpcTimeouts rpccfg.HTTPTimeouts
	EvmCallTimeout  time.Duration
	LogDirVerbosity string
	LogDirPath      string

	BatchLimit      int // Maximum number of requests in a batch
	ReturnDataLimit int // Maximum number of bytes returned from calls (like eth_call)

	// Ots API
	OtsMaxPageSize uint64
}
