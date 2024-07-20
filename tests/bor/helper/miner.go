package helper

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/erigontech/erigon/polygon/bor/borcfg"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/params"
)

// InitGenesis initializes genesis file from json with sprint size and chain name as configurable inputs
func InitGenesis(fileLocation string, sprintSize uint64, chainName string) types.Genesis {
	// sprint size = 8 in genesis
	genesisData, err := os.ReadFile(fileLocation)
	if err != nil {
		panic(err)
	}

	genesis := &types.Genesis{}
	if err := json.Unmarshal(genesisData, genesis); err != nil {
		panic(err)
	}

	genesis.Config.ChainName = chainName

	if genesis.Config.BorJSON != nil {
		borConfig := &borcfg.BorConfig{}
		err = json.Unmarshal(genesis.Config.BorJSON, borConfig)
		if err != nil {
			panic(fmt.Sprintf("Could not parse 'bor' config for %s: %v", fileLocation, err))
		}

		borConfig.Sprint["0"] = sprintSize
		genesis.Config.Bor = borConfig
	}

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

// InitNode initializes a node with the given genesis file and config
func InitMiner(ctx context.Context, genesis *types.Genesis, privKey *ecdsa.PrivateKey, withoutHeimdall bool, minerID int) (*node.Node, *eth.Ethereum, error) {
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
			AllowedPorts:    []uint{30303, 30304, 30305, 30306, 30307, 30308, 30309, 30310},
			PrivateKey:      privKey,
			NAT:             nat.Any(),
		},

		// These are set to prevent disk and page size churn which can be excessive
		// when running multiple nodes
		// MdbxGrowthStep impacts disk usage, MdbxDBSizeLimit impacts page file usage
		MdbxGrowthStep:  4 * datasize.MB,
		MdbxDBSizeLimit: 64 * datasize.MB,
	}

	stack, err := node.New(ctx, nodeCfg, logger)
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

	downloaderConfig, err := downloadercfg.New(datadir.New(ddir), nodeCfg.Version, torrentLogLevel, downloadRate, uploadRate, utils.TorrentPortFlag.Value, utils.TorrentConnsPerFileFlag.Value, utils.TorrentDownloadSlotsFlag.Value, []string{}, []string{}, "", true, utils.DbWriteMapFlag.Value)
	if err != nil {
		return nil, nil, err
	}

	ethCfg := &ethconfig.Config{
		Dirs:      datadir.New(ddir),
		Genesis:   genesis,
		NetworkID: genesis.Config.ChainID.Uint64(),
		TxPool:    txpoolcfg.DefaultConfig,
		GPO:       ethconfig.Defaults.GPO,
		Miner: params.MiningConfig{
			Etherbase:  crypto.PubkeyToAddress(privKey.PublicKey),
			GasLimit:   genesis.GasLimit,
			GasPrice:   big.NewInt(1),
			Recommit:   125 * time.Second,
			SigKey:     privKey,
			Enabled:    true,
			EnabledPOS: true,
		},
		Sync:            ethconfig.Defaults.Sync,
		Downloader:      downloaderConfig,
		WithoutHeimdall: withoutHeimdall,
		ImportMode:      ethconfig.Defaults.ImportMode,

		DeprecatedTxPool: ethconfig.Defaults.DeprecatedTxPool,
		RPCGasCap:        50000000,
		RPCTxFeeCap:      1, // 1 ether
		Snapshot:         ethconfig.BlocksFreezing{NoDownloader: true},
		StateStream:      true,
	}
	ethCfg.TxPool.DBDir = nodeCfg.Dirs.TxPool
	ethCfg.DeprecatedTxPool.CommitEvery = 15 * time.Second
	ethCfg.DeprecatedTxPool.StartOnInit = true
	ethCfg.Downloader.ClientConfig.ListenPort = utils.TorrentPortFlag.Value + minerID
	ethCfg.TxPool.AccountSlots = 1000000
	ethCfg.DeprecatedTxPool.AccountSlots = 1000000
	ethCfg.DeprecatedTxPool.GlobalSlots = 1000000

	ethBackend, err := eth.New(ctx, stack, ethCfg, logger)
	if err != nil {
		return nil, nil, err
	}

	err = ethBackend.Init(stack, ethCfg, ethCfg.Genesis.Config)
	if err != nil {
		return nil, nil, err
	}

	return stack, ethBackend, err
}
