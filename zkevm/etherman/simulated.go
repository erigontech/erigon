package etherman

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/zkevm/etherman/smartcontracts/matic"
	"github.com/ledgerwatch/erigon/zkevm/etherman/smartcontracts/mockverifier"
	"github.com/ledgerwatch/erigon/zkevm/etherman/smartcontracts/polygonzkevm"
	"github.com/ledgerwatch/erigon/zkevm/etherman/smartcontracts/polygonzkevmbridge"
	"github.com/ledgerwatch/erigon/zkevm/etherman/smartcontracts/polygonzkevmglobalexitroot"
)

// NewSimulatedEtherman creates an etherman that uses a simulated blockchain. It's important to notice that the ChainID of the auth
// must be 1337. The address that holds the auth will have an initial balance of 10 ETH
func NewSimulatedEtherman(cfg Config, auth *bind.TransactOpts) (etherman *Client, ethBackend *backends.SimulatedBackend, maticAddr common.Address, br *polygonzkevmbridge.Polygonzkevmbridge, err error) {
	if auth == nil {
		// read only client
		return &Client{}, nil, common.Address{}, nil, nil
	}
	// 10000000 ETH in wei
	balance, _ := new(big.Int).SetString("10000000000000000000000000", 10) //nolint:gomnd
	address := auth.From
	genesisAlloc := map[common.Address]types.GenesisAccount{
		address: {
			Balance: balance,
		},
	}
	blockGasLimit := uint64(999999999999999999) //nolint:gomnd
	client := backends.NewSimulatedBackend(&testing.T{}, genesisAlloc, blockGasLimit)

	// Deploy contracts
	const maticDecimalPlaces = 18
	totalSupply, _ := new(big.Int).SetString("10000000000000000000000000000", 10) //nolint:gomnd
	maticAddr, _, maticContract, err := matic.DeployMatic(auth, client, "Matic Token", "MATIC", maticDecimalPlaces, totalSupply)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	rollupVerifierAddr, _, _, err := mockverifier.DeployMockverifier(auth, client)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	nonce, err := client.PendingNonceAt(context.TODO(), auth.From)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	const posBridge = 1
	calculatedBridgeAddr := crypto.CreateAddress(auth.From, nonce+posBridge)
	const posPoE = 2
	calculatedPoEAddr := crypto.CreateAddress(auth.From, nonce+posPoE)
	genesis := common.HexToHash("0xfd3434cd8f67e59d73488a2b8da242dd1f02849ea5dd99f0ca22c836c3d5b4a9") // Random value. Needs to be different to 0x0
	exitManagerAddr, _, globalExitRoot, err := polygonzkevmglobalexitroot.DeployPolygonzkevmglobalexitroot(auth, client, calculatedPoEAddr, calculatedBridgeAddr)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	bridgeAddr, _, br, err := polygonzkevmbridge.DeployPolygonzkevmbridge(auth, client)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	poeAddr, _, poe, err := polygonzkevm.DeployPolygonzkevm(auth, client, exitManagerAddr, maticAddr, rollupVerifierAddr, bridgeAddr, 1000, 1) //nolint
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	_, err = br.Initialize(auth, 0, exitManagerAddr, poeAddr)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}

	poeParams := polygonzkevm.PolygonZkEVMInitializePackedParameters{
		Admin:                    auth.From,
		TrustedSequencer:         auth.From,
		PendingStateTimeout:      10000, //nolint:gomnd
		TrustedAggregator:        auth.From,
		TrustedAggregatorTimeout: 10000, //nolint:gomnd
	}
	_, err = poe.Initialize(auth, poeParams, genesis, "http://localhost", "L2", "v1") //nolint:gomnd
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}

	if calculatedBridgeAddr != bridgeAddr {
		return nil, nil, common.Address{}, nil, fmt.Errorf("bridgeAddr (%s) is different from the expected contract address (%s)",
			bridgeAddr.String(), calculatedBridgeAddr.String())
	}
	if calculatedPoEAddr != poeAddr {
		return nil, nil, common.Address{}, nil, fmt.Errorf("poeAddr (%s) is different from the expected contract address (%s)",
			poeAddr.String(), calculatedPoEAddr.String())
	}

	// Approve the bridge and poe to spend 10000 matic tokens.
	approvedAmount, _ := new(big.Int).SetString("10000000000000000000000", 10) //nolint:gomnd
	_, err = maticContract.Approve(auth, bridgeAddr, approvedAmount)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	_, err = maticContract.Approve(auth, poeAddr, approvedAmount)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	_, err = poe.ActivateForceBatches(auth)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}

	client.Commit()
	c := &Client{
		EthClient:             client,
		PoE:                   poe,
		Matic:                 maticContract,
		GlobalExitRootManager: globalExitRoot,
		SCAddresses:           []common.Address{poeAddr, exitManagerAddr},
		auth:                  map[common.Address]bind.TransactOpts{},
	}
	err = c.AddOrReplaceAuth(*auth)
	if err != nil {
		return nil, nil, common.Address{}, nil, err
	}
	return c, client, maticAddr, br, nil
}
