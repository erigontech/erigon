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

package devnet

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/cmd/utils"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/erigon-lib/common/dbg"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	devnet_args "github.com/erigontech/erigon/cmd/devnet/args"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	erigonapp "github.com/erigontech/erigon/turbo/app"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
)

type Network struct {
	DataDir            string
	Chain              string
	Logger             log.Logger
	BasePort           int
	BasePrivateApiAddr string
	BaseRPCHost        string
	BaseRPCPort        int
	Snapshots          bool
	Nodes              []Node
	Services           []Service
	Genesis            *types.Genesis
	BorStateSyncDelay  time.Duration
	BorPeriod          time.Duration
	BorMinBlockSize    int
	BorWithMilestones  *bool
	wg                 sync.WaitGroup
	peers              []string
	namedNodes         map[string]Node

	// max number of blocks to look for a transaction in
	MaxNumberOfEmptyBlockChecks int
}

func (nw *Network) ChainID() *big.Int {
	if len(nw.Nodes) > 0 {
		return nw.Nodes[0].ChainID()
	}

	return &big.Int{}
}

// Start starts the process for multiple erigon nodes running on the dev chain
func (nw *Network) Start(ctx context.Context) error {
	for _, service := range nw.Services {
		if err := service.Start(ctx); err != nil {
			nw.Stop()
			return err
		}
	}

	baseNode := devnet_args.NodeArgs{
		DataDir:        nw.DataDir,
		Chain:          nw.Chain,
		Port:           nw.BasePort,
		HttpPort:       nw.BaseRPCPort,
		PrivateApiAddr: nw.BasePrivateApiAddr,
		Snapshots:      nw.Snapshots,
	}

	if nw.BorWithMilestones != nil {
		baseNode.WithHeimdallMilestones = *nw.BorWithMilestones
	} else {
		baseNode.WithHeimdallMilestones = utils.WithHeimdallMilestones.Value
	}

	nw.namedNodes = map[string]Node{}

	for i, nodeArgs := range nw.Nodes {
		{
			baseNode.StaticPeers = strings.Join(nw.peers, ",")

			err := nodeArgs.Configure(baseNode, i)
			if err != nil {
				nw.Stop()
				return err
			}

			node, err := nw.createNode(nodeArgs)
			if err != nil {
				nw.Stop()
				return err
			}

			nw.Nodes[i] = node
			nw.namedNodes[node.GetName()] = node
			nw.peers = append(nw.peers, nodeArgs.GetEnodeURL())

			for _, service := range nw.Services {
				service.NodeCreated(ctx, node)
			}
		}
	}

	for _, node := range nw.Nodes {
		err := nw.startNode(node)
		if err != nil {
			nw.Stop()
			return err
		}

		for _, service := range nw.Services {
			service.NodeStarted(ctx, node)
		}
	}

	return nil
}

var blockProducerFunds = (&big.Int{}).Mul(big.NewInt(1000), big.NewInt(params.Ether))

func (nw *Network) createNode(nodeArgs Node) (Node, error) {
	nodeAddr := fmt.Sprintf("%s:%d", nw.BaseRPCHost, nodeArgs.GetHttpPort())

	n := &devnetNode{
		sync.Mutex{},
		requests.NewRequestGenerator(nodeAddr, nw.Logger),
		nodeArgs,
		&nw.wg,
		nw,
		make(chan error),
		nil,
		nil,
		nil,
	}

	if n.IsBlockProducer() {
		if nw.Genesis == nil {
			nw.Genesis = &types.Genesis{}
		}

		if nw.Genesis.Alloc == nil {
			nw.Genesis.Alloc = types.GenesisAlloc{
				n.Account().Address: types.GenesisAccount{Balance: blockProducerFunds},
			}
		} else {
			nw.Genesis.Alloc[n.Account().Address] = types.GenesisAccount{Balance: blockProducerFunds}
		}
	}

	return n, nil
}

func copyFlags(flags []cli.Flag) []cli.Flag {
	copies := make([]cli.Flag, len(flags))

	for i, flag := range flags {
		flagValue := reflect.ValueOf(flag).Elem()
		copyValue := reflect.New(flagValue.Type()).Elem()

		for f := 0; f < flagValue.NumField(); f++ {
			if flagValue.Type().Field(f).PkgPath == "" {
				copyValue.Field(f).Set(flagValue.Field(f))
			}
		}

		copies[i] = copyValue.Addr().Interface().(cli.Flag)
	}

	return copies
}

// startNode starts an erigon node on the dev chain
func (nw *Network) startNode(n Node) error {
	nw.wg.Add(1)

	node := n.(*devnetNode)

	args, err := devnet_args.AsArgs(node.nodeArgs)
	if err != nil {
		return err
	}

	go func() {
		nw.Logger.Info("Running node", "name", node.GetName(), "args", args)

		// catch any errors and avoid panics if an error occurs
		defer func() {
			panicResult := recover()
			if panicResult == nil {
				return
			}

			nw.Logger.Error("catch panic", "node", node.GetName(), "err", panicResult, "stack", dbg.Stack())
			nw.Stop()
			os.Exit(1)
		}()

		// cli flags are not thread safe and assume only one copy of a flag
		// variable is needed per process - which does not work here
		app := erigonapp.MakeApp(node.GetName(), node.run, copyFlags(erigoncli.DefaultFlags))

		if err := app.Run(args); err != nil {
			nw.Logger.Warn("App run returned error", "node", node.GetName(), "err", err)
		}
	}()

	if err = <-node.startErr; err != nil {
		return err
	}

	return nil
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

	nw.Logger.Info("Stopping services")
	for _, service := range nw.Services {
		service.Stop()
	}

	// TODO should we wait for services
}

func (nw *Network) Wait() {
	nw.wg.Wait()
}

func (nw *Network) FirstNode() Node {
	return nw.Nodes[0]
}

func (nw *Network) SelectNode(ctx context.Context, selector interface{}) Node {
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
