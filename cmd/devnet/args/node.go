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
	BuildDir                  string `arg:"positional" default:"./build/bin/devnet"`
	DataDir                   string `arg:"--datadir" default:"./dev"`
	Chain                     string `arg:"--chain" default:"dev"`
	Port                      int    `arg:"--port"`
	AllowedPorts              string `arg:"--p2p.allowed-ports"`
	NAT                       string `arg:"--nat" default:"none"`
	ConsoleVerbosity          string `arg:"--log.console.verbosity" default:"0"`
	DirVerbosity              string `arg:"--log.dir.verbosity"`
	LogDirPath                string `arg:"--log.dir.path"`
	LogDirPrefix              string `arg:"--log.dir.prefix"`
	P2PProtocol               string `arg:"--p2p.protocol" default:"68"`
	Downloader                string `arg:"--no-downloader" default:"true"`
	WS                        string `arg:"--ws" flag:"" default:"true"`
	PrivateApiAddr            string `arg:"--private.api.addr" default:"localhost:9090"`
	HttpPort                  int    `arg:"--http.port" default:"8545"`
	HttpVHosts                string `arg:"--http.vhosts"`
	AuthRpcPort               int    `arg:"--authrpc.port" default:"8551"`
	AuthRpcVHosts             string `arg:"--authrpc.vhosts"`
	WSPort                    int    `arg:"-" default:"8546"` // flag not defined
	GRPCPort                  int    `arg:"-" default:"8547"` // flag not defined
	TCPPort                   int    `arg:"-" default:"8548"` // flag not defined
	StaticPeers               string `arg:"--staticpeers"`
	WithoutHeimdall           bool   `arg:"--bor.withoutheimdall" flag:"" default:"false"`
}

func (node *Node) configure(base Node, nodeNumber int) error {
	node.DataDir = filepath.Join(base.DataDir, fmt.Sprintf("%d", nodeNumber))

	node.LogDirPath = filepath.Join(base.DataDir, "logs")
	node.LogDirPrefix = fmt.Sprintf("node-%d", nodeNumber)

	node.Chain = base.Chain

	node.StaticPeers = base.StaticPeers

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
	Mine            bool   `arg:"--mine" flag:"true"`
	DevPeriod       int    `arg:"--dev.period"`
	BorPeriod       int    `arg:"--bor.period"`
	BorMinBlockSize int    `arg:"--bor.minblocksize"`
	HttpApi         string `arg:"--http.api" default:"admin,eth,erigon,web3,net,debug,trace,txpool,parity,ots"`
	AccountSlots    int    `arg:"--txpool.accountslots" default:"16"`
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
	HttpApi     string `arg:"--http.api" default:"admin,eth,debug,net,trace,web3,erigon,txpool"`
	TorrentPort string `arg:"--torrent.port" default:"42070"`
	NoDiscover  string `arg:"--nodiscover" flag:"" default:"true"`
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
