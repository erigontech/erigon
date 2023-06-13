package helper

import (
	"crypto/ecdsa"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

func InitGenesis(t *testing.T, fileLocation string, sprintSize uint64) types.Genesis {
	t.Helper()

	// sprint size = 8 in genesis
	genesisData, err := os.ReadFile(fileLocation)
	if err != nil {
		t.Fatalf("%s", err)
	}

	genesis := &types.Genesis{}

	if err := json.Unmarshal(genesisData, genesis); err != nil {
		t.Fatalf("%s", err)
	}

	genesis.Config.Bor.Sprint["0"] = sprintSize
	genesis.Config.ChainName = "bor-e2e-test"

	return *genesis
}

func NewEthConfig() *ethconfig.Config {
	ethConfig := &ethconfig.Defaults
	return ethConfig
}

func NewNodeConfig() *nodecfg.Config {
	nodeConfig := nodecfg.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := params.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit)
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "erigon"
	return &nodeConfig
}

func InitMiner(genesis *types.Genesis, privKey *ecdsa.PrivateKey, withoutHeimdall bool) (*node.Node, *eth.Ethereum, error) {
	// Define the basic configurations for the Ethereum node
	ddir, _ := os.MkdirTemp("", "")

	logger := log.New()

	nodeCfg := &nodecfg.Config{
		Name:    "erigon",
		Version: params.Version,
		Dirs:    datadir.New(ddir),
		P2P: p2p.Config{
			ListenAddr:      ":30303",
			ProtocolVersion: []uint{direct.ETH68, direct.ETH67}, // No need to specify direct.ETH66, because 1 sentry is used for both 66 and 67
			MaxPeers:        100,
			MaxPendingPeers: 1000,
			AllowedPorts:    []uint{30303, 30304, 30305, 30306, 30307},
			PrivateKey:      privKey,
		},
	}

	stack, err := node.New(nodeCfg, logger)
	if err != nil {
		return nil, nil, err
	}

	downloadRate, err := datasize.ParseString("16mb")
	if err != nil {
		return nil, nil, err
	}

	uploadRate, err := datasize.ParseString("4mb")
	if err != nil {
		return nil, nil, err
	}

	torrentLogLevel, _, err := downloadercfg.Int2LogLevel(3)
	if err != nil {
		return nil, nil, err
	}

	downloaderConfig, err := downloadercfg.New(datadir.New(ddir).Snap, nodeCfg.Version, torrentLogLevel, downloadRate, uploadRate, utils.TorrentPortFlag.Value, utils.TorrentConnsPerFileFlag.Value, utils.TorrentDownloadSlotsFlag.Value, []string{})
	if err != nil {
		return nil, nil, err
	}

	ethCfg := &ethconfig.Config{
		Dirs:      datadir.New(ddir),
		Genesis:   genesis,
		NetworkID: genesis.Config.ChainID.Uint64(),
		TxPool:    txpoolcfg.DefaultConfig,
		GPO:       ethconfig.Defaults.GPO,
		Ethash:    ethconfig.Defaults.Ethash,
		Miner: params.MiningConfig{
			Etherbase: crypto.PubkeyToAddress(privKey.PublicKey),
			GasLimit:  genesis.GasLimit * 11 / 10,
			GasPrice:  big.NewInt(1),
			Recommit:  100 * time.Second,
			SigKey:    privKey,
			Enabled:   true,
		},
		Downloader:      downloaderConfig,
		WithoutHeimdall: withoutHeimdall,
	}
	ethCfg.TxPool.DBDir = nodeCfg.Dirs.TxPool

	ethBackend, err := eth.New(stack, ethCfg, logger)
	if err != nil {
		return nil, nil, err
	}

	err = stack.Start()

	return stack, ethBackend, err
}
