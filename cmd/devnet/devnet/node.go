package devnet

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/args"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/diagnostics"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	enode "github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type Node interface {
	requests.RequestGenerator
	Name() string
	ChainID() *big.Int
	Account() *accounts.Account
	IsBlockProducer() bool
}

type NodeSelector interface {
	Test(ctx context.Context, node Node) bool
}

type NodeSelectorFunc func(ctx context.Context, node Node) bool

func (f NodeSelectorFunc) Test(ctx context.Context, node Node) bool {
	return f(ctx, node)
}

func HTTPHost(n Node) string {
	if n, ok := n.(*node); ok {
		host := n.nodeCfg.Http.HttpListenAddress

		if host == "" {
			host = "localhost"
		}

		return fmt.Sprintf("%s:%d", host, n.nodeCfg.Http.HttpPort)
	}

	return ""
}

type node struct {
	sync.Mutex
	requests.RequestGenerator
	args     interface{}
	wg       *sync.WaitGroup
	network  *Network
	startErr chan error
	nodeCfg  *nodecfg.Config
	ethCfg   *ethconfig.Config
	ethNode  *enode.ErigonNode
}

func (n *node) Stop() {
	var toClose *enode.ErigonNode

	n.Lock()
	if n.ethNode != nil {
		toClose = n.ethNode
		n.ethNode = nil
	}
	n.Unlock()

	if toClose != nil {
		toClose.Close()
	}

	n.done()
}

func (n *node) running() bool {
	n.Lock()
	defer n.Unlock()
	return n.startErr == nil && n.ethNode != nil
}

func (n *node) done() {
	n.Lock()
	defer n.Unlock()
	if n.wg != nil {
		wg := n.wg
		n.wg = nil
		wg.Done()
	}
}

func (n *node) IsBlockProducer() bool {
	_, isBlockProducer := n.args.(args.BlockProducer)
	return isBlockProducer
}

func (n *node) Account() *accounts.Account {
	if miner, ok := n.args.(args.BlockProducer); ok {
		return miner.Account()
	}

	return nil
}

func (n *node) Name() string {
	if named, ok := n.args.(interface{ Name() string }); ok {
		return named.Name()
	}

	return ""
}

func (n *node) ChainID() *big.Int {
	if n.ethCfg != nil {
		return n.ethCfg.Genesis.Config.ChainID
	}

	return nil
}

// run configures, creates and serves an erigon node
func (n *node) run(ctx *cli.Context) error {
	var logger log.Logger
	var err error
	var metricsMux *http.ServeMux

	defer n.done()
	defer func() {
		n.Lock()
		if n.startErr != nil {
			close(n.startErr)
			n.startErr = nil
		}
		n.ethNode = nil
		n.Unlock()
	}()

	if logger, metricsMux, err = debug.Setup(ctx, false /* rootLogger */); err != nil {
		return err
	}

	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	n.nodeCfg = enode.NewNodConfigUrfave(ctx, logger)
	n.ethCfg = enode.NewEthConfigUrfave(ctx, n.nodeCfg, logger)

	// These are set to prevent disk and page size churn which can be excessive
	// when running multiple nodes
	// MdbxGrowthStep impacts disk usage, MdbxDBSizeLimit impacts page file usage
	n.nodeCfg.MdbxGrowthStep = 32 * datasize.MB
	n.nodeCfg.MdbxDBSizeLimit = 512 * datasize.MB

	for addr, account := range n.network.Alloc {
		n.ethCfg.Genesis.Alloc[addr] = account
	}

	if n.network.BorStateSyncDelay > 0 {
		n.ethCfg.Bor.StateSyncConfirmationDelay = map[string]uint64{"0": uint64(n.network.BorStateSyncDelay.Seconds())}
	}

	n.ethNode, err = enode.New(ctx.Context, n.nodeCfg, n.ethCfg, logger)

	if metricsMux != nil {
		diagnostics.Setup(ctx, metricsMux, n.ethNode)
	}

	n.Lock()
	if n.startErr != nil {
		n.startErr <- err
		close(n.startErr)
		n.startErr = nil
	}
	n.Unlock()

	if err != nil {
		logger.Error("Node startup", "err", err)
		return err
	}

	err = n.ethNode.Serve()

	if err != nil {
		logger.Error("error while serving Devnet node", "err", err)
	}

	return err
}
