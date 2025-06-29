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

package args

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"strconv"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc/requests"
)

type NodeArgs struct {
	requests.RequestGenerator `arg:"-"`
	Name                      string `arg:"-"`
	BuildDir                  string `arg:"positional" default:"./build/bin/devnet" json:"builddir"`
	DataDir                   string `arg:"--datadir" default:"./dev" json:"datadir"`
	Chain                     string `arg:"--chain" default:"dev" json:"chain"`
	Port                      int    `arg:"--port" json:"port,omitempty"`
	AllowedPorts              string `arg:"--p2p.allowed-ports" json:"p2p.allowed-ports,omitempty"`
	NAT                       string `arg:"--nat" default:"none" json:"nat"`
	ConsoleVerbosity          string `arg:"--log.console.verbosity" default:"0" json:"log.console.verbosity"`
	DirVerbosity              string `arg:"--log.dir.verbosity" json:"log.dir.verbosity,omitempty"`
	LogDirPath                string `arg:"--log.dir.path" json:"log.dir.path,omitempty"`
	LogDirPrefix              string `arg:"--log.dir.prefix" json:"log.dir.prefix,omitempty"`
	P2PProtocol               string `arg:"--p2p.protocol" default:"68" json:"p2p.protocol"`
	Snapshots                 bool   `arg:"--snapshots" flag:"" default:"false" json:"snapshots,omitempty"`
	Downloader                string `arg:"--no-downloader" default:"true" json:"no-downloader"`
	WS                        string `arg:"--ws" flag:"" default:"true" json:"ws"`
	PrivateApiAddr            string `arg:"--private.api.addr" default:"localhost:9090" json:"private.api.addr"`
	HttpPort                  int    `arg:"--http.port" default:"8545" json:"http.port"`
	HttpVHosts                string `arg:"--http.vhosts" json:"http.vhosts"`
	HttpCorsDomain            string `arg:"--http.corsdomain" json:"http.corsdomain"`
	AuthRpcPort               int    `arg:"--authrpc.port" default:"8551" json:"authrpc.port"`
	AuthRpcVHosts             string `arg:"--authrpc.vhosts" json:"authrpc.vhosts"`
	WSPort                    int    `arg:"--ws.port" default:"8546" json:"ws.port"`
	GRPCPort                  int    `arg:"-" default:"8547" json:"-"` // flag not defined
	TCPPort                   int    `arg:"-" default:"8548" json:"-"` // flag not defined
	Metrics                   bool   `arg:"--metrics" flag:"" default:"false" json:"metrics"`
	MetricsPort               int    `arg:"--metrics.port" json:"metrics.port,omitempty"`
	MetricsAddr               string `arg:"--metrics.addr" json:"metrics.addr,omitempty"`
	StaticPeers               string `arg:"--staticpeers" json:"staticpeers,omitempty"`
	WithoutHeimdall           bool   `arg:"--bor.withoutheimdall" flag:"" default:"false" json:"bor.withoutheimdall,omitempty"`
	HeimdallURL               string `arg:"--bor.heimdall" json:"bor.heimdall,omitempty"`
	WithHeimdallMilestones    bool   `arg:"--bor.milestone" json:"bor.milestone"`
	VMDebug                   bool   `arg:"--vmdebug" flag:"" default:"false" json:"dmdebug"`

	NodeKey    *ecdsa.PrivateKey `arg:"-"`
	NodeKeyHex string            `arg:"--nodekeyhex" json:"nodekeyhex,omitempty"`
}

func (node *NodeArgs) Configure(base NodeArgs, nodeNumber int) error {
	if len(node.Name) == 0 {
		node.Name = fmt.Sprintf("%s-%d", base.Chain, nodeNumber)
	}

	node.DataDir = filepath.Join(base.DataDir, node.Name)

	node.LogDirPath = filepath.Join(base.DataDir, "logs")
	node.LogDirPrefix = node.Name

	node.Chain = base.Chain

	node.StaticPeers = base.StaticPeers

	var err error
	node.NodeKey, err = crypto.GenerateKey()
	if err != nil {
		return err
	}
	node.NodeKeyHex = hex.EncodeToString(crypto.FromECDSA(node.NodeKey))

	node.Metrics = base.Metrics
	node.MetricsPort = base.MetricsPort
	node.MetricsAddr = base.MetricsAddr

	node.Snapshots = base.Snapshots

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

	node.Port = base.Port + nodeNumber

	node.WithHeimdallMilestones = base.WithHeimdallMilestones

	return nil
}

func (node *NodeArgs) GetName() string {
	return node.Name
}

func (node *NodeArgs) ChainID() *big.Int {
	spec := params.ChainSpecByName(node.Chain)
	if spec.IsEmpty() {
		return nil
	}
	return spec.Config.ChainID
}

func (node *NodeArgs) GetHttpPort() int {
	return node.HttpPort
}

func (node *NodeArgs) GetEnodeURL() string {
	port := node.Port
	return enode.NewV4(&node.NodeKey.PublicKey, net.ParseIP("127.0.0.1"), port, port).URLv4()
}

func (node *NodeArgs) EnableMetrics(port int) {
	node.Metrics = true
	node.MetricsPort = port
}

type BlockProducer struct {
	NodeArgs
	Mine            bool   `arg:"--mine" flag:"true"`
	Etherbase       string `arg:"--miner.etherbase"`
	GasLimit        int    `arg:"--miner.gaslimit"`
	DevPeriod       int    `arg:"--dev.period"`
	BorPeriod       int    `arg:"--bor.period"`
	BorMinBlockSize int    `arg:"--bor.minblocksize"`
	HttpApi         string `arg:"--http.api" default:"admin,eth,erigon,web3,net,debug,trace,txpool,parity,ots"`
	AccountSlots    int    `arg:"--txpool.accountslots" default:"16"`
	account         *accounts.Account
}

func (m *BlockProducer) Configure(baseNode NodeArgs, nodeNumber int) error {
	err := m.NodeArgs.Configure(baseNode, nodeNumber)
	if err != nil {
		return err
	}

	switch m.Chain {
	case networkname.Dev:
		if m.DevPeriod == 0 {
			m.DevPeriod = 30
		}
		m.account = accounts.NewAccount(m.GetName() + "-etherbase")
		core.DevnetEtherbase = m.account.Address
		core.DevnetSignPrivateKey = m.account.SigKey()

	case networkname.BorDevnet:
		m.account = accounts.NewAccount(m.GetName() + "-etherbase")

		if len(m.HttpApi) == 0 {
			m.HttpApi = "admin,eth,erigon,web3,net,debug,trace,txpool,parity,ots,bor"
		}
	}

	if m.account != nil {
		m.Etherbase = m.account.Address.Hex()
	}

	return nil
}

func (n *BlockProducer) Account() *accounts.Account {
	return n.account
}

func (n *BlockProducer) IsBlockProducer() bool {
	return true
}

type BlockConsumer struct {
	NodeArgs
	HttpApi     string `arg:"--http.api" default:"admin,eth,debug,net,trace,web3,erigon,txpool" json:"http.api"`
	TorrentPort string `arg:"--torrent.port" default:"42070" json:"torrent.port"`
	NoDiscover  string `arg:"--nodiscover" flag:"" default:"true" json:"nodiscover"`
}

func (n *BlockConsumer) IsBlockProducer() bool {
	return false
}

func (n *BlockConsumer) Account() *accounts.Account {
	return nil
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
