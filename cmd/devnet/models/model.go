package models

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rpc"
)

type (
	// TransactionType is the type of transaction attempted to be made, can be regular or contract
	TransactionType string
)

const (
	// MaxNumberOfBlockChecks is the max number of blocks to look for a transaction in
	MaxNumberOfBlockChecks = 3

	// hexPrivateKey is the hex value for the private key
	hexPrivateKey = "26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48"
	// DevAddress is the developer address for sending
	DevAddress = "0x67b1d87101671b127f5f8714789C7192f7ad340e"

	// NonContractTx is the transaction type for sending ether
	NonContractTx TransactionType = "non-contract"
	// ContractTx is the transaction type for sending ether
	ContractTx TransactionType = "contract"
	// DynamicFee is the transaction type for dynamic fee
	DynamicFee TransactionType = "dynamic-fee"

	// SolContractMethodSignature is the function signature for the event in the solidity contract definition
	SolContractMethodSignature = "SubscriptionEvent()"
)

var (
	// DevSignedPrivateKey is the signed private key for signing transactions
	DevSignedPrivateKey, _ = crypto.HexToECDSA(hexPrivateKey)
	// gspec is the geth dev genesis block
	gspec = core.DeveloperGenesisBlock(uint64(0), libcommon.HexToAddress(DevAddress))
	// ContractBackend is a simulated backend created using a simulated blockchain
	ContractBackend = backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, 1_000_000)

	// MethodSubscriptionMap is a container for all the subscription methods
	MethodSubscriptionMap *map[requests.SubMethod]*MethodSubscription

	// NewHeadsChan is the block cache the eth_NewHeads
	NewHeadsChan chan interface{}

	//QuitNodeChan is the channel for receiving a quit signal on all nodes
	QuitNodeChan chan bool
)

// MethodSubscription houses the client subscription, name and channel for its delivery
type MethodSubscription struct {
	Client    *rpc.Client
	ClientSub *rpc.ClientSubscription
	Name      requests.SubMethod
	SubChan   chan interface{}
}

// NewMethodSubscription returns a new MethodSubscription instance
func NewMethodSubscription(name requests.SubMethod) *MethodSubscription {
	return &MethodSubscription{
		Name:    name,
		SubChan: make(chan interface{}),
	}
}

// Block represents a simple block for queries
type Block struct {
	Number       *hexutil.Big
	Transactions []libcommon.Hash
	BlockHash    libcommon.Hash
}
