package devnet

import (
	go_context "context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/devnet/args"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type Network struct {
	DataDir            string
	Chain              string
	Logger             log.Logger
	BasePrivateApiAddr string
	BaseRPCAddr        string
	Snapshots          bool
	Nodes              []Node
	wg                 sync.WaitGroup
	peers              []string
}

// Start starts the process for two erigon nodes running on the dev chain
func (nw *Network) Start(ctx *cli.Context) error {

	type configurable interface {
		Configure(baseNode args.Node, nodeNumber int) (int, interface{}, error)
	}

	apiHost, apiPort, err := net.SplitHostPort(nw.BaseRPCAddr)

	if err != nil {
		return err
	}

	apiPortNo, err := strconv.Atoi(apiPort)

	if err != nil {
		return err
	}

	baseNode := args.Node{
		DataDir:        nw.DataDir,
		Chain:          nw.Chain,
		HttpPort:       apiPortNo,
		PrivateApiAddr: nw.BasePrivateApiAddr,
		Snapshots:      nw.Snapshots,
	}

	metricsEnabled := ctx.Bool("metrics")
	metricsNode := ctx.Int("metrics.node")

	for i, node := range nw.Nodes {
		if configurable, ok := node.(configurable); ok {

			base := baseNode

			if metricsEnabled && metricsNode == i {
				base.Metrics = true
				base.MetricsPort = ctx.Int("metrics.port")
			}

			nodePort, args, err := configurable.Configure(base, i)

			if err == nil {
				node, err = nw.startNode(fmt.Sprintf("http://%s:%d", apiHost, nodePort), args, i)
			}

			if err != nil {
				nw.Stop()
				return err
			}

			nw.Nodes[i] = node

			// get the enode of the node
			// - note this has the side effect of waiting for the node to start
			enode, err := getEnode(node)

			if err != nil {
				if errors.Is(err, devnetutils.ErrInvalidEnodeString) {
					continue
				}

				nw.Stop()
				return err
			}

			nw.peers = append(nw.peers, enode)
			baseNode.StaticPeers = strings.Join(nw.peers, ",")

			// TODO do we need to call AddPeer to the nodes to make them aware of this one
			// the current model only works for an appending node network where the peers gossip
			// connections - not sure if this is the case ?
		}
	}

	return nil
}

// startNode starts an erigon node on the dev chain
func (nw *Network) startNode(nodeAddr string, cfg interface{}, nodeNumber int) (Node, error) {

	args, err := devnetutils.AsArgs(cfg)

	if err != nil {
		return nil, err
	}

	nw.wg.Add(1)

	node := node{
		sync.Mutex{},
		requests.NewRequestGenerator(nodeAddr, nw.Logger),
		cfg,
		&nw.wg,
		make(chan error),
		nil,
	}

	go func() {
		nw.Logger.Info("Running node", "number", nodeNumber, "args", args)

		// catch any errors and avoid panics if an error occurs
		defer func() {
			panicResult := recover()
			if panicResult == nil {
				return
			}

			nw.Logger.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
			nw.Stop()
			os.Exit(1)
		}()

		app := erigonapp.MakeApp(fmt.Sprintf("node-%d", nodeNumber), node.run, erigoncli.DefaultFlags)

		if err := app.Run(args); err != nil {
			nw.Logger.Warn("App run returned error", "node", fmt.Sprintf("node-%d", nodeNumber), "err", err)
		}
	}()

	if err = <-node.startErr; err != nil {
		return nil, err
	}

	return &node, nil
}

// getEnode returns the enode of the netowrk node
func getEnode(n Node) (string, error) {
	reqCount := 0

	for {
		nodeInfo, err := n.AdminNodeInfo()

		if err != nil {
			if r, ok := n.(*node); ok {
				if !r.running() {
					return "", err
				}
			}

			if reqCount < 10 {
				var urlErr *url.Error
				if errors.As(err, &urlErr) {
					var opErr *net.OpError
					if errors.As(urlErr.Err, &opErr) {
						var callErr *os.SyscallError
						if errors.As(opErr.Err, &callErr) {
							if strings.HasPrefix(callErr.Syscall, "connect") {
								reqCount++
								time.Sleep(time.Duration(devnetutils.RandomInt(5)) * time.Second)
								continue
							}
						}
					}
				}
			}

			return "", err
		}

		enode, err := devnetutils.UniqueIDFromEnode(nodeInfo.Enode)

		if err != nil {
			return "", err
		}

		return enode, nil
	}
}

func (nw *Network) Run(ctx go_context.Context, scenario scenarios.Scenario) error {
	return scenarios.Run(WithNetwork(ctx, nw), &scenario)
}

func (nw *Network) Stop() {
	type stoppable interface {
		Stop()
		running() bool
	}

	for i, n := range nw.Nodes {
		if stoppable, ok := n.(stoppable); ok && stoppable.running() {
			nw.Logger.Info("Stopping", "node", i)
			go stoppable.Stop()
		}
	}

	nw.Logger.Info("Waiting for nodes to stop")
	nw.Wait()
}

func (nw *Network) Wait() {
	nw.wg.Wait()
}

func (nw *Network) AnyNode(ctx go_context.Context) Node {
	return nw.SelectNode(ctx, devnetutils.RandomInt(len(nw.Nodes)-1))
}

func (nw *Network) SelectNode(ctx go_context.Context, selector interface{}) Node {
	switch selector := selector.(type) {
	case int:
		if selector < len(nw.Nodes) {
			return nw.Nodes[selector]
		}
	case NodeSelector:
		for _, node := range nw.Nodes {
			if selector.Test(ctx, node) {
				return node
			}
		}
	}

	return nil
}

func (nw *Network) BlockProducers() []Node {
	var blockProducers []Node

	for _, node := range nw.Nodes {
		if node.IsBlockProducer() {
			blockProducers = append(blockProducers, node)
		}
	}

	return blockProducers
}

func (nw *Network) NonBlockProducers() []Node {
	var nonBlockProducers []Node

	for _, node := range nw.Nodes {
		if !node.IsBlockProducer() {
			nonBlockProducers = append(nonBlockProducers, node)
		}
	}

	return nonBlockProducers
}
