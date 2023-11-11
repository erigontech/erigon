package tests

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/args"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	account_services "github.com/ledgerwatch/erigon/cmd/devnet/services/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/polygon"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

func NewBorDevnetWithoutHeimdall(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	logger log.Logger,
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
		Alloc: types.GenesisAlloc{
			faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
		},
		Services: []devnet.Service{
			account_services.NewFaucet(networkname.BorDevnetChainName, faucetSource),
		},
		Nodes: []devnet.Node{
			&args.BlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
					WithoutHeimdall:  true,
				},
				AccountSlots: 200,
			},
			&args.NonBlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
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
	heimdallGrpcAddr string,
	checkpointOwner *accounts.Account,
	withMilestones bool,
	logger log.Logger,
) devnet.Devnet {
	faucetSource := accounts.NewAccount("faucet-source")

	var services []devnet.Service
	if heimdall != nil {
		services = append(services, heimdall)
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
		Alloc: types.GenesisAlloc{
			faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
		},
		Nodes: []devnet.Node{
			&args.BlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
					HeimdallGrpcAddr: heimdallGrpcAddr,
				},
				AccountSlots: 200,
			},
			&args.BlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
					HeimdallGrpcAddr: heimdallGrpcAddr,
				},
				AccountSlots: 200,
			},
			/*&args.BlockProducer{
				Node: args.Node{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
					HeimdallGrpcAddr:     heimdallGrpcAddr,
				},
				AccountSlots: 200,
			},*/
			&args.NonBlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
					HeimdallGrpcAddr: heimdallGrpcAddr,
				},
			},
		},
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
		Alloc: types.GenesisAlloc{
			faucetSource.Address:    {Balance: accounts.EtherAmount(200_000)},
			checkpointOwner.Address: {Balance: accounts.EtherAmount(10_000)},
		},
		Nodes: []devnet.Node{
			&args.BlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "5",
					VMDebug:          true,
					HttpCorsDomain:   "*",
				},
				DevPeriod:    5,
				AccountSlots: 200,
			},
			&args.NonBlockProducer{
				NodeArgs: args.NodeArgs{
					ConsoleVerbosity: "0",
					DirVerbosity:     "3",
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
	logger log.Logger,
) devnet.Devnet {
	heimdallGrpcAddr := ""
	checkpointOwner := accounts.NewAccount("checkpoint-owner")
	withMilestones := utils.WithHeimdallMilestones.Value
	return NewBorDevnetWithHeimdall(
		dataDir,
		baseRpcHost,
		baseRpcPort,
		nil,
		heimdallGrpcAddr,
		checkpointOwner,
		withMilestones,
		logger)
}

func NewBorDevnetWithLocalHeimdall(
	dataDir string,
	baseRpcHost string,
	baseRpcPort int,
	heimdallGrpcAddr string,
	sprintSize uint64,
	logger log.Logger,
) devnet.Devnet {
	config := *params.BorDevnetChainConfig
	if sprintSize > 0 {
		config.Bor.Sprint = map[string]uint64{"0": sprintSize}
	}

	checkpointOwner := accounts.NewAccount("checkpoint-owner")

	heimdall := polygon.NewHeimdall(
		&config,
		heimdallGrpcAddr,
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
		heimdallGrpcAddr,
		checkpointOwner,
		// milestones are not supported yet on the local heimdall
		false,
		logger)
}
