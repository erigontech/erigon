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

package networks

import (
	"strconv"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/cmd/devnet/args"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	account_services "github.com/erigontech/erigon/cmd/devnet/services/accounts"
	"github.com/erigontech/erigon/cmd/devnet/services/polygon"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

func NewBorDevnetWithoutHeimdall(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	gasLimit uint64,
	logger log.Logger,
	consoleLogLevel log.Lvl,
	dirLogLevel log.Lvl,
) devnet.Devnet {
	faucetSource := accounts.NewAccount("faucet-source")

	network := devnet.Network{
		DataDir:            dataDir,
		Chain:              networkname.BorDevnetChainName,
		Logger:             logger,
		BasePort:           40303,
		BasePrivateApiAddr: "localhost:10090",
		BaseRPCHost:        baseRpcHost,
		BaseRPCPort:        baseRpcPort,
		//Snapshots:          true,
		Genesis: &types.Genesis{
			Alloc: types.GenesisAlloc{
				faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
			},
			GasLimit: gasLimit,
		},
		Services: []devnet.Service{
			account_services.NewFaucet(networkname.BorDevnetChainName, faucetSource),
		},
		Nodes: []devnet.Node{
			&args.BlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
					DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
					WithoutHeimdall:  true,
				},
				AccountSlots: 200,
			},
			&args.BlockConsumer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
					DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
					WithoutHeimdall:  true,
				},
			},
		},
	}

	return devnet.Devnet{&network}
}

func NewBorDevnetWithHeimdall(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	heimdall *polygon.Heimdall,
	heimdallURL string,
	checkpointOwner *accounts.Account,
	producerCount int,
	gasLimit uint64,
	withMilestones bool,
	logger log.Logger,
	consoleLogLevel log.Lvl,
	dirLogLevel log.Lvl,
) devnet.Devnet {
	faucetSource := accounts.NewAccount("faucet-source")

	var services []devnet.Service
	if heimdall != nil {
		services = append(services, heimdall)
	}

	var nodes []devnet.Node

	if producerCount == 0 {
		producerCount++
	}

	for i := 0; i < producerCount; i++ {
		nodes = append(nodes, &args.BlockProducer{
			NodeArgs: args.NodeArgs{
				ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
				DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
				HeimdallURL:      heimdallURL,
			},
			AccountSlots: 20000,
		})
	}

	borNetwork := devnet.Network{
		DataDir:            dataDir,
		Chain:              networkname.BorDevnetChainName,
		Logger:             logger,
		BasePort:           40303,
		BasePrivateApiAddr: "localhost:10090",
		BaseRPCHost:        baseRpcHost,
		BaseRPCPort:        baseRpcPort,
		BorStateSyncDelay:  5 * time.Second,
		BorWithMilestones:  &withMilestones,
		Services:           append(services, account_services.NewFaucet(networkname.BorDevnetChainName, faucetSource)),
		Genesis: &types.Genesis{
			Alloc: types.GenesisAlloc{
				faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
			},
			GasLimit: gasLimit,
		},
		Nodes: append(nodes,
			&args.BlockConsumer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
					DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
					HeimdallURL:      heimdallURL,
				},
			}),
	}

	devNetwork := devnet.Network{
		DataDir:            dataDir,
		Chain:              networkname.DevChainName,
		Logger:             logger,
		BasePort:           30403,
		BasePrivateApiAddr: "localhost:10190",
		BaseRPCHost:        baseRpcHost,
		BaseRPCPort:        baseRpcPort + 1000,
		Services:           append(services, account_services.NewFaucet(networkname.DevChainName, faucetSource)),
		Genesis: &types.Genesis{
			Alloc: types.GenesisAlloc{
				faucetSource.Address:    {Balance: accounts.EtherAmount(200_000)},
				checkpointOwner.Address: {Balance: accounts.EtherAmount(10_000)},
			},
		},
		Nodes: []devnet.Node{
			&args.BlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
					DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
					VMDebug:          true,
					HttpCorsDomain:   "*",
				},
				DevPeriod:    5,
				AccountSlots: 200,
			},
			&args.BlockConsumer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
					DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
				},
			},
		},
	}

	return devnet.Devnet{
		&borNetwork,
		&devNetwork,
	}
}

func NewBorDevnetWithRemoteHeimdall(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	producerCount int,
	gasLimit uint64,
	logger log.Logger,
	consoleLogLevel log.Lvl,
	dirLogLevel log.Lvl,
) devnet.Devnet {
	heimdallURL := ""
	checkpointOwner := accounts.NewAccount("checkpoint-owner")
	withMilestones := utils.WithHeimdallMilestones.Value
	return NewBorDevnetWithHeimdall(
		dataDir,
		baseRpcHost,
		baseRpcPort,
		nil,
		heimdallURL,
		checkpointOwner,
		producerCount,
		gasLimit,
		withMilestones,
		logger,
		consoleLogLevel,
		dirLogLevel)
}

func NewBorDevnetWithLocalHeimdall(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	heimdallURL string,
	sprintSize uint64,
	producerCount int,
	gasLimit uint64,
	logger log.Logger,
	consoleLogLevel log.Lvl,
	dirLogLevel log.Lvl,
) devnet.Devnet {
	config := *params.BorDevnetChainConfig
	borConfig := config.Bor.(*borcfg.BorConfig)
	if sprintSize > 0 {
		borConfig.Sprint = map[string]uint64{"0": sprintSize}
	}

	checkpointOwner := accounts.NewAccount("checkpoint-owner")

	heimdall := polygon.NewHeimdall(
		&config,
		heimdallURL,
		&polygon.CheckpointConfig{
			CheckpointBufferTime: 60 * time.Second,
			CheckpointAccount:    checkpointOwner,
		},
		logger)

	return NewBorDevnetWithHeimdall(
		dataDir,
		baseRpcHost,
		baseRpcPort,
		heimdall,
		heimdallURL,
		checkpointOwner,
		producerCount,
		gasLimit,
		// milestones are not supported yet on the local heimdall
		false,
		logger, consoleLogLevel, dirLogLevel)
}
