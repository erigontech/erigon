package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
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
	BlockNumber(ctx context.Context) (uint64, error)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	remoteDbAdddress string
	db               *remote.DB
}

func (api *APIImpl) ensureConnected() error {
	if api.db == nil {
		conn, err := net.Dial("tcp", api.remoteDbAdddress)
		if err != nil {
			return err
		}
		api.db, err = remote.NewDB(conn, conn, conn)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectAPIImpl connects to the remote DB and returns APIImpl instance
func ConnectAPIImpl(remoteDbAdddress string) (*APIImpl, error) {
	return &APIImpl{remoteDbAdddress: remoteDbAdddress}, nil
}

// BlockNumber returns the currently highest block number available in the remote db
func (api *APIImpl) BlockNumber(ctx context.Context) (uint64, error) {
	if err := api.ensureConnected(); err != nil {
		return 0, err
	}
	var blockNumber uint64
	if err := api.db.View(func(tx *remote.Tx) error {
		b := tx.Bucket(dbutils.HeadHeaderKey)
		if b == nil {
			return fmt.Errorf("bucket %s not found", dbutils.HeadHeaderKey)
		}
		blockHashData := b.Get(dbutils.HeadHeaderKey)
		if len(blockHashData) != common.HashLength {
			return fmt.Errorf("head header hash not found or wrong size: %x", blockHashData)
		}
		b1 := tx.Bucket(dbutils.HeaderNumberPrefix)
		if b1 == nil {
			return fmt.Errorf("bucket %s not found", dbutils.HeaderNumberPrefix)
		}
		blockNumberData := b1.Get(blockHashData)
		if len(blockNumberData) != 8 {
			return fmt.Errorf("head block number not found or wrong size: %x", blockNumberData)
		}
		blockNumber = binary.BigEndian.Uint64(blockNumberData)
		return nil
	}); err != nil {
		api.db.Close()
		api.db = nil
		return 0, err
	}
	return blockNumber, nil
}

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	if err := api.ensureConnected(); err != nil {
		return nil, err
	}

	var block *types.Block
	additionalFields := make(map[string]interface{})

	if err := api.db.View(func(tx *remote.Tx) error {
		block = GetBlockByNumber(tx, uint64(number.Int64()))
		additionalFields["totalDifficulty"] = ReadTd(tx, block.Hash(), uint64(number.Int64()))
		return nil
	}); err != nil {
		api.db.Close()
		api.db = nil
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

// ReadTd reimplemented rawdb.ReadTd
func ReadTd(tx *remote.Tx, hash common.Hash, number uint64) *hexutil.Big {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return nil
	}

	data := bucket.Get(dbutils.HeaderTDKey(number, hash))
	if len(data) == 0 {
		return nil
	}
	td := new(big.Int)
	if err := rlp.Decode(bytes.NewReader(data), td); err != nil {
		log.Error("Invalid block total difficulty RLP", "hash", hash, "err", err)
		return nil
	}
	return (*hexutil.Big)(td)
}

// ReadCanonicalHash reimplementation of rawdb.ReadCanonicalHash
func ReadCanonicalHash(tx *remote.Tx, number uint64) common.Hash {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return common.Hash{}
		//return fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
	}

	data := bucket.Get(dbutils.HeaderHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// GetBlockByNumber reimplementation of chain.GetBlockByNumber
func GetBlockByNumber(tx *remote.Tx, number uint64) *types.Block {
	hash := ReadCanonicalHash(tx, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return ReadBlock(tx, hash, number)
}

// ReadBlock reimplementation of rawdb.ReadBlock
func ReadBlock(tx *remote.Tx, hash common.Hash, number uint64) *types.Block {
	header := ReadHeader(tx, hash, number)
	if header == nil {
		return nil
	}
	body := ReadBody(tx, hash, number)
	if body == nil {
		return nil
	}
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
}

// ReadBlock reimplementation of rawdb.ReadBlock
func ReadHeaderRLP(tx *remote.Tx, hash common.Hash, number uint64) rlp.RawValue {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		//return fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
		log.Error("Bucket not founc", "error", dbutils.HeaderPrefix)
		return rlp.RawValue{}
	}
	return bucket.Get(dbutils.HeaderKey(number, hash))
}

// ReadHeader reimplementation of rawdb.ReadHeader
func ReadHeader(tx *remote.Tx, hash common.Hash, number uint64) *types.Header {
	data := ReadHeaderRLP(tx, hash, number)
	if len(data) == 0 {
		return nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		log.Error("Invalid block header RLP", "hash", hash, "err", err)
		return nil
	}
	return header
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(tx *remote.Tx, hash common.Hash, number uint64) rlp.RawValue {
	bucket := tx.Bucket(dbutils.BlockBodyPrefix)
	if bucket == nil {
		//return fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
		log.Error("Bucket not founc", "error", dbutils.BlockBodyPrefix)
		return rlp.RawValue{}
	}
	return bucket.Get(dbutils.BlockBodyKey(number, hash))
}

// ReadBody reimplementation of rawdb.ReadBody
func ReadBody(tx *remote.Tx, hash common.Hash, number uint64) *types.Body {
	data := ReadBodyRLP(tx, hash, number)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	// Post-processing
	body.SendersToTxs()
	return body
}

func daemon(cfg Config) {
	vhosts := splitAndTrim(cfg.rpcVirtualHost)
	cors := splitAndTrim(cfg.rpcCORSDomain)
	enabledApis := splitAndTrim(cfg.rpcAPI)
	var rpcAPI = []rpc.API{}
	apiImpl, err := ConnectAPIImpl(cfg.remoteDbAdddress)
	if err != nil {
		log.Error("Could not connect to remoteDb", "error", err)
		return
	}
	for _, enabledAPI := range enabledApis {
		switch enabledAPI {
		case "eth":
			var api EthAPI
			api = apiImpl
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   api,
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
