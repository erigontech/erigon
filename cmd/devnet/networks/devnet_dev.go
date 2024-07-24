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

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/cmd/devnet/args"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	account_services "github.com/erigontech/erigon/cmd/devnet/services/accounts"
	"github.com/erigontech/erigon/core/types"
)

func NewDevDevnet(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	producerCount int,
	gasLimit uint64,
	logger log.Logger,
	consoleLogLevel log.Lvl,
	dirLogLevel log.Lvl,
) devnet.Devnet {
	faucetSource := accounts.NewAccount("faucet-source")

	var nodes []devnet.Node

	if producerCount == 0 {
		producerCount++
	}

	for i := 0; i < producerCount; i++ {
		nodes = append(nodes, &args.BlockProducer{
			NodeArgs: args.NodeArgs{
				ConsoleVerbosity: strconv.Itoa(int(consoleLogLevel)),
				DirVerbosity:     strconv.Itoa(int(dirLogLevel)),
			},
			AccountSlots: 200,
		})
	}

	network := devnet.Network{
		DataDir:            dataDir,
		Chain:              networkname.DevChainName,
		Logger:             logger,
		BasePrivateApiAddr: "localhost:10090",
		BaseRPCHost:        baseRpcHost,
		BaseRPCPort:        baseRpcPort,
		Genesis: &types.Genesis{
			Alloc: types.GenesisAlloc{
				faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
			},
			GasLimit: gasLimit,
		},
		Services: []devnet.Service{
			account_services.NewFaucet(networkname.DevChainName, faucetSource),
		},
		MaxNumberOfEmptyBlockChecks: 30,
		Nodes: append(nodes,
			&args.BlockConsumer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
				},
			}),
	}

	return devnet.Devnet{&network}
}
