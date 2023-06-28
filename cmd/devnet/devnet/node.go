package devnet

import (
	go_context "context"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/devnet/args"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	enode "github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type Node interface {
	requests.RequestGenerator
	IsMiner() bool
}

type NodeSelector interface {
	Test(ctx go_context.Context, node Node) bool
}

type NodeSelectorFunc func(ctx go_context.Context, node Node) bool

func (f NodeSelectorFunc) Test(ctx go_context.Context, node Node) bool {
	return f(ctx, node)
}

type node struct {
	requests.RequestGenerator
	args    interface{}
	wg      *sync.WaitGroup
	ethNode *enode.ErigonNode
}

func (n *node) Stop() {
	if n.ethNode != nil {
		toClose := n.ethNode
		n.ethNode = nil
		toClose.Close()
	}

	n.done()
}

func (n *node) running() bool {
	return n.ethNode != nil
}

func (n *node) done() {
	if n.wg != nil {
		wg := n.wg
		n.wg = nil
		wg.Done()
	}
}

func (n node) IsMiner() bool {
	_, isMiner := n.args.(args.Miner)
	return isMiner
}

// run configures, creates and serves an erigon node
func (n *node) run(ctx *cli.Context) error {
	var logger log.Logger
	var err error

	defer n.done()

	if logger, err = debug.Setup(ctx, false /* rootLogger */); err != nil {
		return err
	}

	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	nodeCfg := enode.NewNodConfigUrfave(ctx, logger)
	ethCfg := enode.NewEthConfigUrfave(ctx, nodeCfg, logger)

	nodeCfg.MdbxDBSizeLimit = 512 * datasize.MB

	n.ethNode, err = enode.New(nodeCfg, ethCfg, logger)

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
