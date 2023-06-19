package services

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
)

const gasPrice = 912_345_678
const gasAmount = 875_000_000

const (
	// hexPrivateKey is the hex value for the private key
	hexPrivateKey = "26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48"
	// DevAddress is the developer address for sending
	DevAddress = "0x67b1d87101671b127f5f8714789C7192f7ad340e"

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

	signer = types.LatestSigner(params.AllCliqueProtocolChanges)
)

func CreateManyEIP1559TransactionsRefWithBaseFee(ctx context.Context, addr string, startingNonce *uint64, logger log.Logger) ([]types.Transaction, []types.Transaction, error) {
	toAddress := libcommon.HexToAddress(addr)

	baseFeePerGas, err := BaseFeeFromBlock(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	devnet.Logger(ctx).Info("BaseFeePerGas", "val", baseFeePerGas)

	lowerBaseFeeTransactions, higherBaseFeeTransactions, err := signEIP1559TxsLowerAndHigherThanBaseFee2(ctx, 1, 1, baseFeePerGas, startingNonce, toAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerAndHigherThanBaseFee2: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

func CreateManyEIP1559TransactionsRefWithBaseFee2(ctx context.Context, addr string, startingNonce *uint64) ([]types.Transaction, []types.Transaction, error) {
	toAddress := libcommon.HexToAddress(addr)

	baseFeePerGas, err := BaseFeeFromBlock(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	devnet.Logger(ctx).Info("BaseFeePerGas2", "val", baseFeePerGas)

	lowerBaseFeeTransactions, higherBaseFeeTransactions, err := signEIP1559TxsLowerAndHigherThanBaseFee2(ctx, 100, 100, baseFeePerGas, startingNonce, toAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerAndHigherThanBaseFee2: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

// createNonContractTx returns a signed transaction and the recipient address
func CreateTransaction(addr string, value, nonce uint64) (types.Transaction, libcommon.Address, error) {
	toAddress := libcommon.HexToAddress(addr)

	// create a new transaction using the parameters to send
	transaction := types.NewTransaction(nonce, toAddress, uint256.NewInt(value), params.TxGas, uint256.NewInt(gasPrice), nil)

	// sign the transaction using the developer 0signed private key
	signedTx, err := types.SignTx(transaction, *signer, DevSignedPrivateKey)
	if err != nil {
		return nil, libcommon.Address{}, fmt.Errorf("failed to sign non-contract transaction: %v", err)
	}

	return signedTx, toAddress, nil
}

func signEIP1559TxsLowerAndHigherThanBaseFee2(ctx context.Context, amountLower, amountHigher int, baseFeePerGas uint64, nonce *uint64, toAddress libcommon.Address) ([]types.Transaction, []types.Transaction, error) {
	higherBaseFeeTransactions, err := signEIP1559TxsHigherThanBaseFee(ctx, amountHigher, baseFeePerGas, nonce, toAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsHigherThanBaseFee: %v", err)
	}

	lowerBaseFeeTransactions, err := signEIP1559TxsLowerThanBaseFee(ctx, amountLower, baseFeePerGas, nonce, toAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerThanBaseFee: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

// signEIP1559TxsLowerThanBaseFee creates n number of transactions with gasFeeCap lower than baseFeePerGas
func signEIP1559TxsLowerThanBaseFee(ctx context.Context, n int, baseFeePerGas uint64, nonce *uint64, toAddress libcommon.Address) ([]types.Transaction, error) {
	var signedTransactions []types.Transaction

	var (
		minFeeCap = baseFeePerGas - 300_000_000
		maxFeeCap = (baseFeePerGas - 100_000_000) + 1 // we want the value to be inclusive in the random number generation, hence the addition of 1
	)

	for i := 0; i < n; i++ {
		gasFeeCap, err := devnetutils.RandomNumberInRange(minFeeCap, maxFeeCap)
		if err != nil {
			return nil, err
		}

		value, err := devnetutils.RandomNumberInRange(0, 100_000)
		if err != nil {
			return nil, err
		}

		transaction := types.NewEIP1559Transaction(*signer.ChainID(), *nonce, toAddress, uint256.NewInt(value), uint64(210_000), uint256.NewInt(gasPrice), new(uint256.Int), uint256.NewInt(gasFeeCap), nil)

		devnet.Logger(ctx).Info("LOWER", "transaction", i, "nonce", transaction.Nonce, "value", transaction.Value, "feecap", transaction.FeeCap)

		signedTransaction, err := types.SignTx(transaction, *signer, DevSignedPrivateKey)

		if err != nil {
			return nil, err
		}

		signedTransactions = append(signedTransactions, signedTransaction)
		*nonce++
	}

	return signedTransactions, nil
}

// signEIP1559TxsHigherThanBaseFee creates amount number of transactions with gasFeeCap higher than baseFeePerGas
func signEIP1559TxsHigherThanBaseFee(ctx context.Context, n int, baseFeePerGas uint64, nonce *uint64, toAddress libcommon.Address) ([]types.Transaction, error) {
	var signedTransactions []types.Transaction

	var (
		minFeeCap = baseFeePerGas
		maxFeeCap = (baseFeePerGas + 100_000_000) + 1 // we want the value to be inclusive in the random number generation, hence the addition of 1
	)

	for i := 0; i < n; i++ {
		gasFeeCap, err := devnetutils.RandomNumberInRange(minFeeCap, maxFeeCap)
		if err != nil {
			return nil, err
		}

		value, err := devnetutils.RandomNumberInRange(0, 100_000)
		if err != nil {
			return nil, err
		}

		transaction := types.NewEIP1559Transaction(*signer.ChainID(), *nonce, toAddress, uint256.NewInt(value), uint64(210_000), uint256.NewInt(gasPrice), new(uint256.Int), uint256.NewInt(gasFeeCap), nil)

		devnet.Logger(ctx).Info("HIGHER", "transaction", i, "nonce", transaction.Nonce, "value", transaction.Value, "feecap", transaction.FeeCap)

		signedTransaction, err := types.SignTx(transaction, *signer, DevSignedPrivateKey)
		if err != nil {
			return nil, err
		}

		signedTransactions = append(signedTransactions, signedTransaction)
		*nonce++
	}

	return signedTransactions, nil
}

// DeploySubsriptionContract creates and signs a transaction using the developer address, returns the contract and the signed transaction
func DeploySubsriptionContract(nonce uint64) (types.Transaction, libcommon.Address, *contracts.Subscription, *bind.TransactOpts, error) {
	// initialize transactOpts
	transactOpts, err := initializeTransactOps(nonce)
	if err != nil {
		return nil, libcommon.Address{}, nil, nil, fmt.Errorf("failed to initialize transactOpts: %v", err)
	}

	// deploy the contract and get the contract handler
	address, txToSign, subscriptionContract, err := contracts.DeploySubscription(transactOpts, ContractBackend)
	if err != nil {
		return nil, libcommon.Address{}, nil, nil, fmt.Errorf("failed to deploy subscription: %v", err)
	}

	// sign the transaction with the private key
	signedTx, err := types.SignTx(txToSign, *signer, DevSignedPrivateKey)
	if err != nil {
		return nil, libcommon.Address{}, nil, nil, fmt.Errorf("failed to sign tx: %v", err)
	}

	return signedTx, address, subscriptionContract, transactOpts, nil
}

// initializeTransactOps initializes the transactOpts object for a contract transaction
func initializeTransactOps(nonce uint64) (*bind.TransactOpts, error) {
	var chainID = big.NewInt(1337)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(DevSignedPrivateKey, chainID)
	if err != nil {
		return nil, fmt.Errorf("cannot create transactor with chainID %d, error: %v", chainID, err)
	}

	transactOpts.GasLimit = uint64(200_000)
	transactOpts.GasPrice = big.NewInt(880_000_000)
	transactOpts.Nonce = big.NewInt(int64(nonce)) // TODO: Get Nonce from account automatically

	return transactOpts, nil
}

// Block represents a simple block for queries
type Block struct {
	Number       *hexutil.Big
	Transactions []libcommon.Hash
	BlockHash    libcommon.Hash
}

// txHashInBlock checks if the block with block number has the transaction hash in its list of transactions
func txHashInBlock(client *rpc.Client, hashmap map[libcommon.Hash]bool, blockNumber string, txToBlockMap map[libcommon.Hash]string, logger log.Logger) (uint64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // releases the resources held by the context

	var (
		currBlock Block
		numFound  int
	)
	err := client.CallContext(ctx, &currBlock, string(requests.Methods.ETHGetBlockByNumber), blockNumber, false)
	if err != nil {
		return uint64(0), 0, fmt.Errorf("failed to get block by number: %v", err)
	}

	for _, txnHash := range currBlock.Transactions {
		// check if tx is in the hash set and remove it from the set if it is present
		if _, ok := hashmap[txnHash]; ok {
			numFound++
			logger.Info("SUCCESS => Tx included into block", "txHash", txnHash, "blockNum", blockNumber)
			// add the block number as an entry to the map
			txToBlockMap[txnHash] = blockNumber
			delete(hashmap, txnHash)
			if len(hashmap) == 0 {
				return devnetutils.HexToInt(blockNumber), numFound, nil
			}
		}
	}

	return uint64(0), 0, nil
}

// EmitFallbackEvent emits an event from the contract using the fallback method
func EmitFallbackEvent(node devnet.Node, subContract *contracts.Subscription, opts *bind.TransactOpts, logger log.Logger) (*libcommon.Hash, error) {
	logger.Info("EMITTING EVENT FROM FALLBACK...")

	// adding one to the nonce before initiating another transaction
	opts.Nonce.Add(opts.Nonce, big.NewInt(1))

	tx, err := subContract.Fallback(opts, []byte{})
	if err != nil {
		return nil, fmt.Errorf("failed to emit event from fallback: %v", err)
	}

	signedTx, err := types.SignTx(tx, *signer, DevSignedPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign fallback transaction: %v", err)
	}

	hash, err := node.SendTransaction(signedTx)
	if err != nil {
		return nil, fmt.Errorf("failed to send fallback transaction: %v", err)
	}

	return hash, nil
}

func BaseFeeFromBlock(ctx context.Context) (uint64, error) {
	var val uint64
	res, err := devnet.SelectNode(ctx).GetBlockByNumberDetails("latest", false)
	if err != nil {
		return 0, fmt.Errorf("failed to get base fee from block: %v\n", err)
	}

	if v, ok := res["baseFeePerGas"]; !ok {
		return val, fmt.Errorf("baseFeePerGas field missing from response")
	} else {
		val = devnetutils.HexToInt(v.(string))
	}

	return val, err
}

func SendManyTransactions(ctx context.Context, signedTransactions []types.Transaction) ([]*libcommon.Hash, error) {
	logger := devnet.Logger(ctx)

	logger.Info("Sending multiple transactions to the txpool...")
	hashes := make([]*libcommon.Hash, len(signedTransactions))

	for idx, tx := range signedTransactions {
		hash, err := devnet.SelectNode(ctx).SendTransaction(tx)
		if err != nil {
			logger.Error("failed SendTransaction", "error", err)
			//return nil, err
		}
		hashes[idx] = hash
	}

	return hashes, nil
}
