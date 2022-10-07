package models

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/p2p"
)

const (
	// BuildDirArg is the build directory for the devnet executable
	BuildDirArg = "./build/bin/devnet"
	// DataDirArg is the datadir flag
	DataDirArg = "--datadir"
	// ChainArg is the chain flag
	ChainArg = "--chain"
	// DevPeriodArg is the dev.period flag
	DevPeriodArg = "--dev.period"
	// VerbosityArg is the verbosity flag
	VerbosityArg = "--verbosity"
	// Mine is the mine flag
	Mine = "--mine"
	// NoDiscover is the nodiscover flag
	NoDiscover = "--nodiscover"
	// PrivateApiAddrArg is the private.api.addr flag
	PrivateApiAddrArg = "--private.api.addr"
	// StaticPeersArg is the staticpeers flag
	StaticPeersArg = "--staticpeers"
	// HttpApiArg is the http.api flag
	HttpApiArg = "--http.api"

	// DataDirParam is the datadir parameter
	DataDirParam = "./dev"
	// ChainParam is the chain parameter
	ChainParam = "dev"
	// DevPeriodParam is the dev.period parameter
	DevPeriodParam = "30"
	// VerbosityParam is the verbosity parameter
	VerbosityParam = "0"
	// PrivateApiParamMine is the private.api.addr parameter for the mining node
	PrivateApiParamMine = "localhost:9090"
	// PrivateApiParamNoMine is the private.api.addr parameter for the non-mining node
	PrivateApiParamNoMine = "localhost:9091"

	// ErigonUrl is the default url for rpc connections
	ErigonUrl = "http://localhost:8545"
	// ErigonLogFilePrefix is the default file prefix for logging erigon node info and errors
	ErigonLogFilePrefix = "erigon_node_"

	// ReqId is the request id for each request
	ReqId = 0

	// BlockNumLatest is the parameter for the latest block
	BlockNumLatest = "latest"
)

type AdminNodeInfoResponse struct {
	rpctest.CommonResponse
	Result p2p.NodeInfo `json:"result"`
}

// ParameterFromArgument merges the argument and parameter and returns a flag input string
func ParameterFromArgument(arg, param string) (string, error) {
	if arg == "" {
		return "", ErrInvalidArgument
	}
	return fmt.Sprintf("%s=%s", arg, param), nil
}
