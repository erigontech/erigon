package node

import (
	"math"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/params"

	"github.com/urfave/cli"

	gopsutil "github.com/shirou/gopsutil/mem"
)

type TurboGethNode struct {
	stack   *node.Node
	backend *eth.Ethereum
}

func (tg *TurboGethNode) Serve() error {
	defer tg.stack.Close()

	tg.run()

	tg.stack.Wait()

	return nil
}

func (tg *TurboGethNode) run() {
	utils.StartNode(tg.stack)
	// we don't have accounts locally and we don't do mining
	// so these parts are ignored
	// see cmd/geth/main.go#startNode for full implementation
}

type Params struct {
	GitCommit     string
	CustomBuckets dbutils.BucketsCfg
}

func New(
	ctx *cli.Context,
	sync *stagedsync.StagedSync,
	p Params,
) *TurboGethNode {
	prepareBuckets(p.CustomBuckets)
	prepare(ctx)
	nodeConfig := makeNodeConfig(ctx, p)
	node := makeConfigNode(nodeConfig)
	ethConfig := makeEthConfig(ctx, node)

	ethConfig.StagedSync = sync

	ethereum := utils.RegisterEthService(node, ethConfig)

	return &TurboGethNode{stack: node, backend: ethereum}
}

func makeEthConfig(ctx *cli.Context, node *node.Node) *eth.Config {
	ethConfig := &eth.DefaultConfig
	utils.SetEthConfig(ctx, node, ethConfig)
	return ethConfig
}

func makeNodeConfig(ctx *cli.Context, p Params) *node.Config {
	nodeConfig := node.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := p.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit, "")
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "turbo-geth"
	nodeConfig.NoUSB = true

	utils.SetNodeConfig(ctx, &nodeConfig)

	return &nodeConfig
}

func makeConfigNode(config *node.Config) *node.Node {
	stack, err := node.New(config)
	if err != nil {
		utils.Fatalf("Failed to create turbo-geth node: %v", err)
	}

	return stack
}

// prepare manipulates memory cache allowance and setups metric system.
// This function should be called before launching devp2p stack.
func prepare(ctx *cli.Context) {
	// If we're running a known preset, log it for convenience.
	switch {
	case ctx.GlobalIsSet(utils.LegacyTestnetFlag.Name):
		log.Info("Starting Turbo-Geth on Ropsten testnet...")
		log.Warn("The --testnet flag is ambiguous! Please specify one of --goerli, --rinkeby, or --ropsten.")
		log.Warn("The generic --testnet flag is deprecated and will be removed in the future!")

	case ctx.GlobalIsSet(utils.RopstenFlag.Name):
		log.Info("Starting Turbo-Geth on Ropsten testnet...")

	case ctx.GlobalIsSet(utils.RinkebyFlag.Name):
		log.Info("Starting Turbo-Geth on Rinkeby testnet...")

	case ctx.GlobalIsSet(utils.GoerliFlag.Name):
		log.Info("Starting Turbo-Geth on Görli testnet...")

	case ctx.GlobalIsSet(utils.DeveloperFlag.Name):
		log.Info("Starting Turbo-Geth in ephemeral dev mode...")

	case !ctx.GlobalIsSet(utils.NetworkIdFlag.Name):
		log.Info("Starting Turbo-Geth on Ethereum mainnet...")
	}
	// If we're a full node on mainnet without --cache specified, bump default cache allowance
	if !ctx.GlobalIsSet(utils.CacheFlag.Name) && !ctx.GlobalIsSet(utils.NetworkIdFlag.Name) {
		// Make sure we're not on any supported preconfigured testnet either
		if !ctx.GlobalIsSet(utils.LegacyTestnetFlag.Name) && !ctx.GlobalIsSet(utils.RopstenFlag.Name) && !ctx.GlobalIsSet(utils.RinkebyFlag.Name) && !ctx.GlobalIsSet(utils.GoerliFlag.Name) && !ctx.GlobalIsSet(utils.DeveloperFlag.Name) {
			// Nope, we're really on mainnet. Bump that cache up!
			log.Info("Bumping default cache on mainnet", "provided", ctx.GlobalInt(utils.CacheFlag.Name), "updated", 4096)
			ctx.GlobalSet(utils.CacheFlag.Name, strconv.Itoa(4096)) //nolint:errcheck
		}
	}
	// If we're running a light client on any network, drop the cache to some meaningfully low amount
	if !ctx.GlobalIsSet(utils.CacheFlag.Name) {
		log.Info("Dropping default light client cache", "provided", ctx.GlobalInt(utils.CacheFlag.Name), "updated", 128)
		ctx.GlobalSet(utils.CacheFlag.Name, strconv.Itoa(128)) //nolint:errcheck
	}
	// Cap the cache allowance and tune the garbage collector
	mem, err := gopsutil.VirtualMemory()
	if err == nil {
		if 32<<(^uintptr(0)>>63) == 32 && mem.Total > 2*1024*1024*1024 {
			log.Warn("Lowering memory allowance on 32bit arch", "available", mem.Total/1024/1024, "addressable", 2*1024)
			mem.Total = 2 * 1024 * 1024 * 1024
		}
		allowance := int(mem.Total / 1024 / 1024 / 3)
		if cache := ctx.GlobalInt(utils.CacheFlag.Name); cache > allowance {
			log.Warn("Sanitizing cache to Go's GC limits", "provided", cache, "updated", allowance)
			if err = ctx.GlobalSet(utils.CacheFlag.Name, strconv.Itoa(allowance)); err != nil {
				log.Error("Error while sanitizing cache to Go's GC limits", "err", err)
			}
		}
	}
	// Ensure Go's GC ignores the database cache for trigger percentage
	cache := ctx.GlobalInt(utils.CacheFlag.Name)
	gogc := math.Max(20, math.Min(100, 100/(float64(cache)/1024)))

	log.Debug("Sanitizing Go's GC trigger", "percent", int(gogc))
	debug.SetGCPercent(int(gogc))

	// Start metrics export if enabled
	utils.SetupMetrics(ctx)

	// Start system runtime metrics collection
	go metrics.CollectProcessMetrics(3 * time.Second)
}
