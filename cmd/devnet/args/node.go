package args

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"

	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/params/networkname"
)

type Node struct {
	requests.RequestGenerator `arg:"-"`
	BuildDir                  string `arg:"positional" default:"./build/bin/devnet" json:"builddir"`
	DataDir                   string `arg:"--datadir" default:"./dev" json:"--datadir"`
	Chain                     string `arg:"--chain" default:"dev" json:"--chain"`
	Port                      int    `arg:"--port" json:"--port,omitempty"`
	AllowedPorts              string `arg:"--p2p.allowed-ports" json:"--p2p.allowed-ports,omitempty"`
	NAT                       string `arg:"--nat" default:"none" json:"--nat"`
	ConsoleVerbosity          string `arg:"--log.console.verbosity" default:"0" json:"--log.console.verbosity"`
	DirVerbosity              string `arg:"--log.dir.verbosity" json:"--log.dir.verbosity,omitempty"`
	LogDirPath                string `arg:"--log.dir.path" json:"--log.dir.path,omitempty"`
	LogDirPrefix              string `arg:"--log.dir.prefix" json:"--log.dir.prefix,omitempty"`
	P2PProtocol               string `arg:"--p2p.protocol" default:"68" json:"--p2p.protocol"`
	Downloader                string `arg:"--no-downloader" default:"true" json:"--no-downloader"`
	WS                        string `arg:"--ws" flag:"" default:"true" json:"--ws"`
	PrivateApiAddr            string `arg:"--private.api.addr" default:"localhost:9090" json:"--private.api.addr"`
	HttpPort                  int    `arg:"--http.port" default:"8545" json:"--http.port"`
	HttpVHosts                string `arg:"--http.vhosts" json:"--http.vhosts"`
	AuthRpcPort               int    `arg:"--authrpc.port" default:"8551" json:"--authrpc.port"`
	AuthRpcVHosts             string `arg:"--authrpc.vhosts" json:"--authrpc.vhosts"`
	WSPort                    int    `arg:"-" default:"8546" json:"-"` // flag not defined
	GRPCPort                  int    `arg:"-" default:"8547" json:"-"` // flag not defined
	TCPPort                   int    `arg:"-" default:"8548" json:"-"` // flag not defined
	Metrics                   bool   `arg:"--metrics" flag:"" default:"false" json:"--metrics"`
	MetricsPort               int    `arg:"--metrics.port" json:"--metrics.port,omitempty"`
	MetricsAddr               string `arg:"--metrics.addr" json:"--metrics.addr,omitempty"`
	StaticPeers               string `arg:"--staticpeers" json:"--staticpeers,omitempty"`
	WithoutHeimdall           bool   `arg:"--bor.withoutheimdall" flag:"" default:"false" json:"--bor.withoutheimdall,omitempty"`
}

func (node *Node) configure(base Node, nodeNumber int) error {
	node.DataDir = filepath.Join(base.DataDir, fmt.Sprintf("%d", nodeNumber))

	node.LogDirPath = filepath.Join(base.DataDir, "logs")
	node.LogDirPrefix = fmt.Sprintf("node-%d", nodeNumber)

	node.Chain = base.Chain

	node.StaticPeers = base.StaticPeers

	node.Metrics = base.Metrics
	node.MetricsPort = base.MetricsPort
	node.MetricsAddr = base.MetricsAddr

	var err error

	node.PrivateApiAddr, _, err = portFromBase(base.PrivateApiAddr, nodeNumber, 1)

	if err != nil {
		return err
	}

	apiPort := base.HttpPort + (nodeNumber * 5)

	node.HttpPort = apiPort
	node.WSPort = apiPort + 1
	node.GRPCPort = apiPort + 2
	node.TCPPort = apiPort + 3
	node.AuthRpcPort = apiPort + 4

	return nil
}

type Miner struct {
	Node
	Mine            bool   `arg:"--mine" flag:"true" json:"--mine"`
	DevPeriod       int    `arg:"--dev.period" json:"--dev.period"`
	BorPeriod       int    `arg:"--bor.period" json:"--bor.period"`
	BorMinBlockSize int    `arg:"--bor.minblocksize" json:"--bor.minblocksize"`
	HttpApi         string `arg:"--http.api" default:"admin,eth,erigon,web3,net,debug,trace,txpool,parity,ots" json:"--http.api"`
	AccountSlots    int    `arg:"--txpool.accountslots" default:"16" json:"--txpool.accountslots"`
}

func (m Miner) Configure(baseNode Node, nodeNumber int) (int, interface{}, error) {
	err := m.configure(baseNode, nodeNumber)

	if err != nil {
		return -1, nil, err
	}

	switch m.Chain {
	case networkname.DevChainName:
		if m.DevPeriod == 0 {
			m.DevPeriod = 30
		}
	}

	return m.HttpPort, m, nil
}

func (n Miner) IsMiner() bool {
	return true
}

type NonMiner struct {
	Node
	HttpApi     string `arg:"--http.api" default:"admin,eth,debug,net,trace,web3,erigon,txpool" json:"--http.api"`
	TorrentPort string `arg:"--torrent.port" default:"42070" json:"--torrent.port"`
	NoDiscover  string `arg:"--nodiscover" flag:"" default:"true" json:"--nodiscover"`
}

func (n NonMiner) Configure(baseNode Node, nodeNumber int) (int, interface{}, error) {
	err := n.configure(baseNode, nodeNumber)

	if err != nil {
		return -1, nil, err
	}

	return n.HttpPort, n, nil
}

func (n NonMiner) IsMiner() bool {
	return false
}

func portFromBase(baseAddr string, increment int, portCount int) (string, int, error) {
	apiHost, apiPort, err := net.SplitHostPort(baseAddr)

	if err != nil {
		return "", -1, err
	}

	portNo, err := strconv.Atoi(apiPort)

	if err != nil {
		return "", -1, err
	}

	portNo += (increment * portCount)

	return fmt.Sprintf("%s:%d", apiHost, portNo), portNo, nil
}
