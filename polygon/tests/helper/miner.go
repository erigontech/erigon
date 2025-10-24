package helper

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
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

// InitMiner initializes a node with the given genesis file and config
func InitMiner(
	ctx context.Context,
	logger log.Logger,
	dirName string,
	genesis *types.Genesis,
	privKey *ecdsa.PrivateKey,
	withoutHeimdall bool,
) (_ *node.Node, _ *eth.Ethereum, err error) {
	// Define the basic configurations for the Ethereum node

	nodeCfg := &nodecfg.Config{
		Name:    "erigon",
		Version: version.VersionNoMeta,
		Dirs:    datadir.New(dirName),
		P2P: p2p.Config{
			ListenAddr:      ":0",
			ProtocolVersion: []uint{direct.ETH68},
			AllowedPorts:    []uint{0},
			NoDiscovery:     true,
			NoDial:          true,
			MaxPeers:        1,
			MaxPendingPeers: 1,
			PrivateKey:      privKey,
			NAT:             nat.Any(),
		},

		// These are set to prevent disk and page size churn which can be excessive
		// when running multiple nodes
		// MdbxGrowthStep impacts disk usage, MdbxDBSizeLimit impacts page file usage
		MdbxGrowthStep:  4 * datasize.MB,
		MdbxDBSizeLimit: 64 * datasize.MB,
		HTTPTimeouts:    rpccfg.DefaultHTTPTimeouts,
	}

	stack, err := node.New(ctx, nodeCfg, logger)
	if err != nil {
		return nil, nil, err
	}

	downloadRate, err := utils.GetStringFlagRateLimit("16mb")
	if err != nil {
		return
	}

	uploadRate, err := utils.GetStringFlagRateLimit("4mb")
	if err != nil {
		return
	}

	torrentLogLevel, err := downloadercfg.Int2LogLevel(3)
	if err != nil {
		return nil, nil, err
	}

	downloaderConfig, err := downloadercfg.New(
		ctx,
		datadir.New(dirName),
		nodeCfg.Version,
		torrentLogLevel,
		0,
		utils.TorrentConnsPerFileFlag.Value,
		[]string{},
		"",
		utils.DbWriteMapFlag.Value,
		downloadercfg.NewCfgOpts{
			DownloadRateLimit: downloadRate.TorrentRateLimit(),
			UploadRateLimit:   uploadRate.TorrentRateLimit(),
		},
	)
	if err != nil {
		return nil, nil, err
	}

	ethCfg := &ethconfig.Config{
		Dirs:      datadir.New(dirName),
		Genesis:   genesis,
		NetworkID: genesis.Config.ChainID.Uint64(),
		TxPool:    txpoolcfg.DefaultConfig,
		GPO:       ethconfig.Defaults.GPO,
		Miner: buildercfg.MiningConfig{
			Etherbase:  crypto.PubkeyToAddress(privKey.PublicKey),
			GasLimit:   &genesis.GasLimit,
			GasPrice:   big.NewInt(1),
			Recommit:   ethconfig.Defaults.Miner.Recommit,
			SigKey:     privKey,
			Enabled:    true,
			EnabledPOS: true,
		},
		Sync:            ethconfig.Defaults.Sync,
		Downloader:      downloaderConfig,
		WithoutHeimdall: withoutHeimdall,
		ImportMode:      ethconfig.Defaults.ImportMode,
		RPCGasCap:       50000000,
		RPCTxFeeCap:     1, // 1 ether
		Snapshot:        ethconfig.BlocksFreezing{NoDownloader: true, ChainName: genesis.Config.ChainName},
		StateStream:     true,
	}
	ethCfg.TxPool.DBDir = nodeCfg.Dirs.TxPool
	ethCfg.TxPool.CommitEvery = 15 * time.Second
	ethCfg.Downloader.ClientConfig.ListenPort = 0
	ethCfg.TxPool.AccountSlots = 1000000
	ethCfg.TxPool.PendingSubPoolLimit = 1000000

	ethBackend, err := eth.New(ctx, stack, ethCfg, logger, nil)
	if err != nil {
		return nil, nil, err
	}

	err = ethBackend.Init(stack, ethCfg, ethCfg.Genesis.Config)
	if err != nil {
		return nil, nil, err
	}

	return stack, ethBackend, err
}
