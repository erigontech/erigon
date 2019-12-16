package commands

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// splitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func splitAndTrim(input string) []string {
	result := strings.Split(input, ",")
	for i, r := range result {
		result[i] = strings.TrimSpace(r)
	}
	return result
}

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db *remote.DB
}

// PrivateDebugAPI
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex int, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (eth.StorageRangeResult, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	db       *remote.DB
	dbReader ethdb.Getter
}

// ConnectAPIImpl connects to the remote DB and returns APIImpl instance
func ConnectAPIImpl(db *remote.DB) *APIImpl {
	return &APIImpl{
		db: db,
	}
}

// ConnectAPIImpl connects to the remote DB and returns APIImpl instance
func ConnectPrivateDebugAPIImpl(db *remote.DB) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		db:       db,
		dbReader: remote.NewRemoteBoltDatabase(db),
	}
}

func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	var blockNumber uint64

	err := api.db.View(ctx, func(tx *remote.Tx) error {
		b, err := tx.Bucket(dbutils.HeadHeaderKey)
		if err != nil {
			return err
		}

		if b == nil {
			return fmt.Errorf("bucket %s not found", dbutils.HeadHeaderKey)
		}
		blockHashData, err := b.Get(dbutils.HeadHeaderKey)
		if err != nil {
			return err
		}
		if len(blockHashData) != common.HashLength {
			return fmt.Errorf("head header hash not found or wrong size: %x", blockHashData)
		}
		b1, err := tx.Bucket(dbutils.HeaderNumberPrefix)
		if err != nil {
			return err
		}

		if b1 == nil {
			return fmt.Errorf("bucket %s not found", dbutils.HeaderNumberPrefix)
		}
		blockNumberData, err := b1.Get(blockHashData)
		if err != nil {
			return err
		}

		if len(blockNumberData) != 8 {
			return fmt.Errorf("head block number not found or wrong size: %x", blockNumberData)
		}
		blockNumber = binary.BigEndian.Uint64(blockNumberData)
		return nil
	})
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(blockNumber), nil
}

type chainContext struct {
	db rawdb.DatabaseReader
}
type powEngine struct {
}

func (c *powEngine) VerifyHeader(chain consensus.ChainReader, header *types.Header, seal bool) error {

	panic("must not be called")
}
func (c *powEngine) VerifyHeaders(chain consensus.ChainReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	panic("must not be called")
}
func (c *powEngine) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	panic("must not be called")
}
func (c *powEngine) VerifySeal(chain consensus.ChainReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Prepare(chain consensus.ChainReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Finalize(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header) {
	panic("must not be called")
}
func (c *powEngine) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction,
	uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	panic("must not be called")
}
func (c *powEngine) Seal(chain consensus.ChainReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	panic("must not be called")
}
func (c *powEngine) SealHash(header *types.Header) common.Hash {
	panic("must not be called")
}
func (c *powEngine) CalcDifficulty(chain consensus.ChainReader, time uint64, parent *types.Header) *big.Int {
	panic("must not be called")
}
func (c *powEngine) APIs(chain consensus.ChainReader) []rpc.API {
	panic("must not be called")
}

func (c *powEngine) Close() error {
	panic("must not be called")
}

func (c *powEngine) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func (c *chainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(c.db, hash, number)
}
func (c *chainContext) Engine() consensus.Engine {
	return &powEngine{}
}

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	var err error
	var block *types.Block
	additionalFields := make(map[string]interface{})

	err = api.db.View(ctx, func(tx *remote.Tx) error {
		block, err = remote.GetBlockByNumber(tx, uint64(number.Int64()))
		if err != nil {
			return err
		}
		additionalFields["totalDifficulty"], err = remote.ReadTd(tx, block.Hash(), uint64(number.Int64()))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if block != nil {
		response, err := api.rpcMarshalBlock(block, true, fullTx, additionalFields)

		if err == nil && number == rpc.PendingBlockNumber {
			// Pending blocks need to nil out a few fields
			for _, field := range []string{"hash", "nonce", "miner"} {
				response[field] = nil
			}
		}
		return response, err
	}
	return nil, nil
}

// StorageRangeAt re-implementation of eth/api.go:StorageRangeAt
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex int, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (eth.StorageRangeResult, error) {
	block := rawdb.ReadBlockByHash(api.dbReader, blockHash)
	if block == nil {
		return eth.StorageRangeResult{}, fmt.Errorf("block %x not found", blockHash)
	}
	parent := rawdb.ReadBlock(api.dbReader, block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return eth.StorageRangeResult{}, fmt.Errorf("block %x not found", block.ParentHash())
	}

	_, _, _, dbstate, _, err := api.computeTxEnv(block.Hash(), len(block.Transactions())-1)
	if err != nil {
		return eth.StorageRangeResult{}, err
	}

	//dbstate.SetBlockNr(block.NumberU64())
	//statedb.CommitBlock(api.eth.chainConfig.IsEIP158(block.Number()), dbstate)
	return eth.StorageRangeAt(dbstate, contractAddress, keyStart, maxResult)
}

// computeTxEnv returns the execution environment of a certain transaction.
func (api *PrivateDebugAPIImpl) computeTxEnv(blockHash common.Hash, txIndex int) (core.Message, vm.Context, *state.IntraBlockState, *state.DbState, uint64, error) {
	// Create the parent state database
	block := rawdb.ReadBlockByHash(api.dbReader, blockHash)
	if block == nil {
		return nil, vm.Context{}, nil, nil, 0, fmt.Errorf("block %x not found", blockHash)
	}
	parent := rawdb.ReadBlock(api.dbReader, block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.Context{}, nil, nil, 0, fmt.Errorf("parent %x not found", block.ParentHash())
	}
	statedb, dbstate := api.computeIntraBlockState(parent)
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(params.MainnetChainConfig, block.Number())

	for idx, tx := range block.Transactions() {
		// Assemble the transaction call message and return if the requested offset
		msg, _ := tx.AsMessage(signer)
		EVMcontext := core.NewEVMContext(msg, block.Header(), &chainContext{db: api.dbReader}, nil)
		if idx == txIndex {
			return msg, EVMcontext, statedb, dbstate, parent.NumberU64(), nil
		}
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(EVMcontext, statedb, params.MainnetChainConfig, vm.Config{})
		if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
			return nil, vm.Context{}, nil, nil, 0, fmt.Errorf("transaction %x failed: %v", tx.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.Number()), dbstate)
	}
	return nil, vm.Context{}, nil, nil, 0, fmt.Errorf("transaction index %d out of range for block %x", txIndex, blockHash)
}

// computeIntraBlockState retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func (api *PrivateDebugAPIImpl) computeIntraBlockState(block *types.Block) (*state.IntraBlockState, *state.DbState) {
	// If we have the state fully available, use that
	dbstate := state.NewDbState(api.dbReader, block.NumberU64())
	statedb := state.New(dbstate)
	return statedb, dbstate
}

// rpcMarshalBlock reimplementation of ethapi.rpcMarshalBlock
func (api *APIImpl) rpcMarshalBlock(b *types.Block, inclTx bool, fullTx bool, additional map[string]interface{}) (map[string]interface{}, error) {
	fields, err := ethapi.RPCMarshalBlock(b, inclTx, fullTx)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}

func daemon(cfg Config) {
	vhosts := splitAndTrim(cfg.rpcVirtualHost)
	cors := splitAndTrim(cfg.rpcCORSDomain)
	enabledApis := splitAndTrim(cfg.rpcAPI)

	dial := func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error) {
		dialer := net.Dialer{}
		conn, err := dialer.DialContext(ctx, "tcp", cfg.remoteDbAddress)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not connect to remoteDb. addr: %s. err: %w", cfg.remoteDbAddress, err)
		}
		return conn, conn, conn, nil
	}
	db, err := remote.NewDB(context.Background(), dial)
	if err != nil {
		log.Error("Could not connect to remoteDb", "error", err)
		return
	}

	var rpcAPI = []rpc.API{}
	apiImpl := ConnectAPIImpl(db)
	dbgAPIImpl := ConnectPrivateDebugAPIImpl(db)

	for _, enabledAPI := range enabledApis {
		switch enabledAPI {
		case "eth":
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(apiImpl),
				Version:   "1.0",
			})
		case "debug":
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(dbgAPIImpl),
				Version:   "1.0",
			})

		default:
			log.Error("Unrecognised", "api", enabledAPI)
		}
	}
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.rpcListenAddress, cfg.rpcPort)
	listener, _, err := rpc.StartHTTPEndpoint(httpEndpoint, rpcAPI, enabledApis, cors, vhosts, rpc.DefaultHTTPTimeouts)
	if err != nil {
		log.Error("Could not start RPC api", "error", err)
		return
	}
	extapiURL := fmt.Sprintf("http://%s", httpEndpoint)
	log.Info("HTTP endpoint opened", "url", extapiURL)

	defer func() {
		listener.Close()
		log.Info("HTTP endpoint closed", "url", httpEndpoint)
	}()

	abortChan := make(chan os.Signal, 1)
	signal.Notify(abortChan, os.Interrupt)

	sig := <-abortChan
	log.Info("Exiting...", "signal", sig)
}
