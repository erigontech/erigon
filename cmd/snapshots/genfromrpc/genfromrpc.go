package genfromrpc

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"
)

var RpcAddr = cli.StringFlag{
	Name:     "rpcaddr",
	Usage:    `Rpc address to scrape`,
	Required: true,
}

var Command = cli.Command{
	Action: func(cliCtx *cli.Context) error {
		return genFromRPc(cliCtx)
	},
	Name:  "genfromrpc",
	Usage: "genfromrpc utilities",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		//&utils.ChainFlag,
		&RpcAddr,
	},
	Description: ``,
}

type BlockJson struct {
	types.Header

	Uncles       []*types.Header          `json:"uncles"`
	Withdrawals  types.Withdrawals        `json:"withdrawals"`
	Transactions []map[string]interface{} `json:"transactions"`
	BlockHash    common.Hash              `json:"blockHash"`
}

func convertHexToBigInt(hex string) *big.Int {
	bigInt := new(big.Int)
	bigInt.SetString(hex[2:], 16)
	return bigInt
}

func makeLegacyTx(commonTx *types.CommonTx, rawTx map[string]interface{}) *types.LegacyTx {
	legacyTx := &types.LegacyTx{CommonTx: types.CommonTx{
		Nonce: commonTx.Nonce,
		Gas:   commonTx.Gas,
		To:    commonTx.To,
		Value: commonTx.Value,
		Data:  commonTx.Data,
		V:     commonTx.V,
		R:     commonTx.R,
		S:     commonTx.S,
	}}
	gasPriceStr, ok := rawTx["gasPrice"].(string)
	if ok {
		legacyTx.GasPrice = new(uint256.Int)
		legacyTx.GasPrice.SetFromBig(convertHexToBigInt(gasPriceStr))
	}
	return legacyTx
}

func decodeAccessList(rawAccessList []map[string]interface{}) types.AccessList {
	var accessList types.AccessList
	for _, rawSlot := range rawAccessList {
		accessTuple := types.AccessTuple{}
		addressStr := rawSlot["address"].(string)
		accessTuple.Address = common.HexToAddress(addressStr)
		storageKeys := rawSlot["storageKeys"].([]string)
		for _, keyStr := range storageKeys {
			key := common.HexToHash(keyStr)
			accessTuple.StorageKeys = append(accessTuple.StorageKeys, key)
		}
		accessList = append(accessList, accessTuple)
	}
	return accessList
}

func makeAccessListTx(commonTx *types.CommonTx, rawTx map[string]interface{}) *types.AccessListTx {
	var (
		gasPriceStr, ok       = rawTx["gasPrice"].(string)
		chainIdStr, okChainId = rawTx["chainId"].(string)
	)
	var gasPrice *uint256.Int
	if ok {
		gasPrice = new(uint256.Int)
		gasPrice.SetFromBig(convertHexToBigInt(gasPriceStr))
	}

	accessListTx := &types.AccessListTx{LegacyTx: types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce: commonTx.Nonce,
			Gas:   commonTx.Gas,
			To:    commonTx.To,
			Value: commonTx.Value,
			Data:  commonTx.Data,
			V:     commonTx.V,
			R:     commonTx.R,
			S:     commonTx.S,
		},
		GasPrice: gasPrice,
	}}
	if okChainId {
		accessListTx.ChainID = new(uint256.Int)
		accessListTx.ChainID.SetFromBig(convertHexToBigInt(chainIdStr))
	}
	accessListTx.AccessList = decodeAccessList(rawTx["accessList"].([]map[string]interface{}))
	return accessListTx
}

func makeEip1559Tx(commonTx *types.CommonTx, rawTx map[string]interface{}) *types.DynamicFeeTransaction {
	var (
		gasPriceStr, ok       = rawTx["gasPrice"].(string)
		chainIdStr, okChainId = rawTx["chainId"].(string)
		tip, okTip            = rawTx["maxPriorityFeePerGas"].(string)
		feeCap, okFeeCap      = rawTx["maxFeePerGas"].(string)
	)
	var gasPrice *uint256.Int
	if ok {
		gasPrice = new(uint256.Int)
		gasPrice.SetFromBig(convertHexToBigInt(gasPriceStr))
	}

	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce: commonTx.Nonce,
			Gas:   commonTx.Gas,
			To:    commonTx.To,
			Value: commonTx.Value,
			Data:  commonTx.Data,
			V:     commonTx.V,
			R:     commonTx.R,
			S:     commonTx.S,
		},
	}
	if okChainId {
		tx.ChainID = new(uint256.Int)
		tx.ChainID.SetFromBig(convertHexToBigInt(chainIdStr))
	}
	tx.AccessList = decodeAccessList(rawTx["accessList"].([]map[string]interface{}))
	if okTip {
		tx.Tip = new(uint256.Int)
		tx.Tip.SetFromBig(convertHexToBigInt(tip))
	}
	if okFeeCap {
		tx.FeeCap = new(uint256.Int)
		tx.FeeCap.SetFromBig(convertHexToBigInt(feeCap))
	}

	return tx
}

func decodeBlobVersionedHashes(rawVersionedHashes []string) []common.Hash {
	var versionedHashes []common.Hash
	for _, hashStr := range rawVersionedHashes {
		hash := common.HexToHash(hashStr)
		versionedHashes = append(versionedHashes, hash)
	}
	return versionedHashes
}

func makeEip4844Tx(commonTx *types.CommonTx, rawTx map[string]interface{}) *types.BlobTx {
	var (
		gasPriceStr, ok                      = rawTx["gasPrice"].(string)
		chainIdStr, okChainId                = rawTx["chainId"].(string)
		tip, okTip                           = rawTx["maxPriorityFeePerGas"].(string)
		feeCap, okFeeCap                     = rawTx["maxFeePerGas"].(string)
		maxFeePerBlobGas, okMaxFeePerBlobGas = rawTx["maxFeePerBlobGas"].(string)
	)
	var gasPrice *uint256.Int
	if ok {
		gasPrice = new(uint256.Int)
		gasPrice.SetFromBig(convertHexToBigInt(gasPriceStr))
	}

	tx := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce: commonTx.Nonce,
				Gas:   commonTx.Gas,
				To:    commonTx.To,
				Value: commonTx.Value,
				Data:  commonTx.Data,
				V:     commonTx.V,
				R:     commonTx.R,
				S:     commonTx.S,
			},
		},
	}
	if okChainId {
		tx.ChainID = new(uint256.Int)
		tx.ChainID.SetFromBig(convertHexToBigInt(chainIdStr))
	}
	tx.AccessList = decodeAccessList(rawTx["accessList"].([]map[string]interface{}))
	if okTip {
		tx.Tip = new(uint256.Int)
		tx.Tip.SetFromBig(convertHexToBigInt(tip))
	}
	if okFeeCap {
		tx.FeeCap = new(uint256.Int)
		tx.FeeCap.SetFromBig(convertHexToBigInt(feeCap))
	}
	if okMaxFeePerBlobGas {
		tx.MaxFeePerBlobGas = new(uint256.Int)
		tx.MaxFeePerBlobGas.SetFromBig(convertHexToBigInt(maxFeePerBlobGas))
	}
	tx.BlobVersionedHashes = decodeBlobVersionedHashes(rawTx["blobVersionedHashes"].([]string))
	return tx
}

func makeEip7702Tx(commonTx *types.CommonTx, rawTx map[string]interface{}) *types.SetCodeTransaction {
	var (
		gasPriceStr, ok       = rawTx["gasPrice"].(string)
		chainIdStr, okChainId = rawTx["chainId"].(string)
		tip, okTip            = rawTx["maxPriorityFeePerGas"].(string)
		feeCap, okFeeCap      = rawTx["maxFeePerGas"].(string)
	)
	var gasPrice *uint256.Int
	if ok {
		gasPrice = new(uint256.Int)
		gasPrice.SetFromBig(convertHexToBigInt(gasPriceStr))
	}

	tx := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce: commonTx.Nonce,
				Gas:   commonTx.Gas,
				To:    commonTx.To,
				Value: commonTx.Value,
				Data:  commonTx.Data,
				V:     commonTx.V,
				R:     commonTx.R,
				S:     commonTx.S,
			},
		},
	}

	if okChainId {
		tx.ChainID = new(uint256.Int)
		tx.ChainID.SetFromBig(convertHexToBigInt(chainIdStr))
	}
	tx.AccessList = decodeAccessList(rawTx["accessList"].([]map[string]interface{}))
	if okTip {
		tx.Tip = new(uint256.Int)
		tx.Tip.SetFromBig(convertHexToBigInt(tip))
	}
	if okFeeCap {
		tx.FeeCap = new(uint256.Int)
		tx.FeeCap.SetFromBig(convertHexToBigInt(feeCap))
	}

	panic("not sure how to implement")

	return tx
}

func unMarshalTransactions(rawTxs []map[string]interface{}) (types.Transactions, error) {
	var txs types.Transactions
	for _, rawTx := range rawTxs {
		var tx types.Transaction
		status := rawTx["status"].(string)

		// each field is an hex string
		var (
			nonceStr          = rawTx["nonce"].(string)
			gasStr            = rawTx["gas"].(string)
			toStr, okTo       = rawTx["to"].(string)
			valueStr, okValue = rawTx["value"].(string)
			inputStr          = rawTx["input"].(string)
			vStr, okV         = rawTx["v"].(string)
			rStr, okR         = rawTx["r"].(string)
			sStr, okS         = rawTx["s"].(string)
		)

		commonTx := types.CommonTx{}
		commonTx.Nonce = convertHexToBigInt(nonceStr).Uint64()
		commonTx.Gas = convertHexToBigInt(gasStr).Uint64()
		if okTo {
			commonTx.To = new(common.Address)
			*commonTx.To = common.HexToAddress(toStr)
		}
		if okValue {
			commonTx.Value = new(uint256.Int)
		}
		commonTx.Value.SetFromBig(convertHexToBigInt(valueStr))
		commonTx.Data = common.Hex2Bytes(inputStr)
		if okV {
			commonTx.V.SetFromBig(convertHexToBigInt(vStr))
		}
		if okR {
			commonTx.R.SetFromBig(convertHexToBigInt(rStr))
		}
		if okS {
			commonTx.S.SetFromBig(convertHexToBigInt(sStr))
		}

		switch status {
		case "0x0": // legacy
			tx = makeLegacyTx(&commonTx, rawTx)
		case "0x1": // access list
			tx = makeAccessListTx(&commonTx, rawTx)
		case "0x2": // eip1559
			tx = makeEip1559Tx(&commonTx, rawTx)
		case "0x3": // eip4844
			tx = makeEip4844Tx(&commonTx, rawTx)
		case "0x4": // eip7702
			tx = makeEip7702Tx(&commonTx, rawTx)
		default:
			panic("unknown tx type")
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// Obtain block by number and decode it from json
func getBlockByNumber(client *rpc.Client, blockNumber *big.Int) (*types.Block, error) {
	var block BlockJson
	err := client.CallContext(context.Background(), &block, "eth_getBlockByNumber", fmt.Sprintf("0x%x", blockNumber), true)
	if err != nil {
		return nil, err
	}
	txs, err := unMarshalTransactions(block.Transactions)
	if err != nil {
		return nil, err
	}
	blk := types.NewBlockFromNetwork(&block.Header, &types.Body{
		Transactions: txs,
		Uncles:       block.Uncles,
		Withdrawals:  block.Withdrawals,
	})

	if blk.Hash() != block.BlockHash {
		return nil, fmt.Errorf("block hash mismatch, expected %s, got %s. num=%d", blk.Hash(), block.BlockHash, blockNumber)
	}
	return blk, nil
}

func genFromRPc(cliCtx *cli.Context) error {
	jsonRpcAddr := cliCtx.String(RpcAddr.Name)
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(log.LvlInfo), log.StderrHandler))
	// Connect to Arbitrum One RPC
	client, err := rpc.Dial(jsonRpcAddr, log.Root())
	if err != nil {
		log.Warn("Error connecting to RPC", "err", err)
		return err
	}

	// Query latest block number
	var latestBlockHex string
	err = client.CallContext(context.Background(), &latestBlockHex, "eth_blockNumber")
	if err != nil {
		log.Warn("Error fetching latest block number", "err", err)
		return err
	}

	// Convert block number from hex to integer
	latestBlock := new(big.Int)
	latestBlock.SetString(latestBlockHex[2:], 16)

	fmt.Printf("Latest Block: %d\n", latestBlock)
	//
	blk, err := getBlockByNumber(client, common.Big2)
	if err != nil {
		log.Warn("Error fetching block", "err", err)
		return err
	}
	fmt.Println("received", blk.NumberU64(), blk.Hash())
	return nil
	// Loop through last 10 blocks
	// for i := uint64(0); i < latestBlock.Uint64(); i++ {
	// 	blockNumber := new(big.Int).Sub(latestBlock, big.NewInt(int64(i)))
	// 	block, err := getBlockByNumber(client, blockNumber)
	// 	if err != nil {
	// 		log.Error("Error fetching block", "blockNumber", blockNumber, "err", err)
	// 		return err
	// 	}

	// 	// Print block details
	// 	blockJSON, _ := json.MarshalIndent(block, "", "  ")
	// 	fmt.Println(string(blockJSON))
	// }
}
