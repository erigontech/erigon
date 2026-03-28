// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package httpcfg

import (
	"time"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// SharedApiConfig holds all fields consumed when constructing JSON-RPC API
// implementation objects. These are API-layer concerns, not transport concerns.
type SharedApiConfig struct {
	API                               []string
	Gascap                            uint64
	Feecap                            float64
	BlockRangeLimit                   int
	GetLogsMaxResults                 int
	MaxTraces                         uint64
	ReturnDataLimit                   int
	AllowUnprotectedTxs               bool
	MaxGetProofRewindBlockCount       int
	WebsocketSubscribeLogsChannelSize int
	RpcTxSyncDefaultTimeout           time.Duration
	RpcTxSyncMaxTimeout               time.Duration
	OtsMaxPageSize                    uint64
	WithDatadir                       bool
	DataDir                           string
	Dirs                              datadir.Dirs
	StateCache                        kvcache.CoherentConfig
	Snap                              ethconfig.BlocksFreezing
	Sync                              ethconfig.Sync
	DBReadConcurrency                 int
	RpcFiltersConfig                  rpchelper.FiltersConfig
	GraphQLEnabled                    bool
	GethCompatibility                 bool
	TraceCompatibility                bool
	EvmCallTimeout                    time.Duration
	OverlayGetLogsTimeout             time.Duration
	OverlayReplayBlockTimeout         time.Duration
	RpcAllowListFilePath              string
	RpcBatchConcurrency               uint
	RpcStreamingDisable               bool
	BatchLimit                        int
	TraceRequests                     bool
	DebugSingleRequest                bool
	RPCSlowLogThreshold               time.Duration
	TestingEnabled                    bool
	LogDirVerbosity                   string
	LogDirPath                        string
	TxPoolApiAddr                     string
	PrivateApiAddr                    string // outbound dial address for remote services
}

// HttpServerConfig holds HTTP and HTTPS transport configuration.
type HttpServerConfig struct {
	HttpServerEnabled  bool
	HttpURL            string
	HttpListenAddress  string
	HttpPort           int
	HttpCORSDomain     []string
	HttpVirtualHost    []string
	HttpCompression    bool
	HTTPTimeouts       rpccfg.HTTPTimeouts
	HttpsServerEnabled bool
	HttpsURL           string
	HttpsListenAddress string
	HttpsPort          int
	HttpsCertfile      string
	HttpsKeyFile       string
	TLSCertfile        string
	TLSCACert          string
	TLSKeyFile         string
}

// WsConfig holds WebSocket transport configuration.
type WsConfig struct {
	WebsocketEnabled     bool
	WebsocketPort        int
	WebsocketCompression bool
}

// EngineApiConfig holds Engine/Auth RPC configuration.
type EngineApiConfig struct {
	AuthRpcHTTPListenAddress string
	AuthRpcPort              int
	AuthRpcVirtualHost       []string
	AuthRpcTimeouts          rpccfg.HTTPTimeouts
	JWTSecretPath            string
}

// GrpcConfig holds private gRPC server configuration.
type GrpcConfig struct {
	GRPCServerEnabled      bool
	GRPCListenAddress      string
	GRPCPort               int
	GRPCHealthCheckEnabled bool
}

// SocketConfig holds Unix socket server configuration.
type SocketConfig struct {
	SocketServerEnabled bool
	SocketListenUrl     string
}

// HttpCfg is the composite configuration for all RPC subsystems.
// All existing field accesses (e.g. cfg.HttpPort, cfg.Gascap, cfg.AuthRpcPort)
// continue to work via Go struct embedding — no changes to flag-binding code
// or the many call sites that pass *HttpCfg are required.
type HttpCfg struct {
	Enabled bool
	SharedApiConfig
	HttpServerConfig
	WsConfig
	EngineApiConfig
	GrpcConfig
	SocketConfig
}
