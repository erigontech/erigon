package httpcfg

import (
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
)

type HttpCfg struct {
	Enabled                 bool
	PrivateApiAddr          string
	WithDatadir             bool // Erigon's database can be read by separated processes on same machine - in read-only mode - with full support of transactions. It will share same "OS PageCache" with Erigon process.
	DataDir                 string
	Dirs                    datadir.Dirs
	HttpListenAddress       string
	EngineHTTPListenAddress string
	TLSCertfile             string
	TLSCACert               string
	TLSKeyFile              string
	HttpPort                int
	EnginePort              int
	HttpCORSDomain          []string
	HttpVirtualHost         []string
	HttpCompression         bool
	API                     []string
	Gascap                  uint64
	MaxTraces               uint64
	WebsocketEnabled        bool
	WebsocketCompression    bool
	RpcAllowListFilePath    string
	RpcBatchConcurrency     uint
	DBReadConcurrency       int
	TraceCompatibility      bool // Bug for bug compatibility for trace_ routines with OpenEthereum
	TxPoolApiAddr           string
	TevmEnabled             bool
	StateCache              kvcache.CoherentConfig
	Snap                    ethconfig.Snapshot
	Sync                    ethconfig.Sync
	GRPCServerEnabled       bool
	GRPCListenAddress       string
	GRPCPort                int
	GRPCHealthCheckEnabled  bool
	StarknetGRPCAddress     string
	JWTSecretPath           string // Engine API Authentication
	TraceRequests           bool   // Always trace requests in INFO level
}
