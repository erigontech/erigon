package genfromrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
)

// CLI flags
var RpcAddr = cli.StringFlag{
	Name:     "rpcaddr",
	Usage:    `Rpc address to scrape`,
	Required: true,
}

var Verify = cli.BoolFlag{
	Name:  "verify",
	Usage: "Verify block hash",
	Value: true,
}

var FromBlock = cli.Uint64Flag{
	Name:  "from",
	Usage: "Block number to start from",
	Value: 0,
}

var NoWrite = cli.BoolFlag{
	Name:  "no-write",
	Usage: "Avoid writing to the database",
	Value: false,
}

var Arbitrum = cli.BoolFlag{
	Name:  "arbitrum", // this shit wants me to use their shitty tx format.
	Usage: "Avoid writing to the database",
	Value: false,
}

var Command = cli.Command{
	Action:      func(cliCtx *cli.Context) error { return genFromRPc(cliCtx) },
	Name:        "genfromrpc",
	Usage:       "genfromrpc utilities",
	Flags:       []cli.Flag{&utils.DataDirFlag, &RpcAddr, &Verify, &FromBlock, &Arbitrum},
	Description: ``,
}

// BlockJson is the JSON representation of a block returned from the RPC.
type BlockJson struct {
	BlkHash     common.Hash      `json:"hash"`
	ParentHash  common.Hash      `json:"parentHash"       gencodec:"required"`
	UncleHash   common.Hash      `json:"sha3Uncles"       gencodec:"required"`
	Coinbase    common.Address   `json:"miner"`
	Root        common.Hash      `json:"stateRoot"        gencodec:"required"`
	TxHash      common.Hash      `json:"transactionsRoot" gencodec:"required"`
	ReceiptHash common.Hash      `json:"receiptsRoot"     gencodec:"required"`
	Bloom       types.Bloom      `json:"logsBloom"        gencodec:"required"`
	Difficulty  *hexutil.Big     `json:"difficulty"       gencodec:"required"`
	Number      *hexutil.Big     `json:"number"           gencodec:"required"`
	GasLimit    hexutil.Uint64   `json:"gasLimit"         gencodec:"required"`
	GasUsed     hexutil.Uint64   `json:"gasUsed"          gencodec:"required"`
	Time        hexutil.Uint64   `json:"timestamp"        gencodec:"required"`
	Extra       hexutil.Bytes    `json:"extraData"        gencodec:"required"`
	MixDigest   common.Hash      `json:"mixHash"` // prevRandao after EIP-4399
	Nonce       types.BlockNonce `json:"nonce"`

	BaseFee         *hexutil.Big    `json:"baseFeePerGas"`   // EIP-1559
	WithdrawalsHash *common.Hash    `json:"withdrawalsRoot"` // EIP-4895
	BlobGasUsed     *hexutil.Uint64 `json:"blobGasUsed"`
	ExcessBlobGas   *hexutil.Uint64 `json:"excessBlobGas"`

	ParentBeaconBlockRoot *common.Hash `json:"parentBeaconBlockRoot"` // EIP-4788
	RequestsHash          *common.Hash `json:"requestsHash"`          // EIP-7685

	Uncles       []*types.Header          `json:"uncles"`
	Withdrawals  types.Withdrawals        `json:"withdrawals"`
	Transactions []map[string]interface{} `json:"transactions"`
}

// --- Helper functions ---

// convertHexToBigInt converts a hex string (with a "0x" prefix) to a *big.Int.
func convertHexToBigInt(hexStr string) *big.Int {
	bi := new(big.Int)
	// Assumes hexStr starts with "0x"
	bi.SetString(hexStr[2:], 16)
	return bi
}

// getUint256FromField returns a *uint256.Int from the rawTx field if present.
func getUint256FromField(rawTx map[string]interface{}, field string) *uint256.Int {
	if val, ok := rawTx[field].(string); ok {
		i := new(uint256.Int)
		i.SetFromBig(convertHexToBigInt(val))
		return i
	}
	return nil
}

// buildDynamicFeeFields sets the common dynamic fee fields from rawTx.
func buildDynamicFeeFields(tx *types.DynamicFeeTransaction, rawTx map[string]interface{}) {
	if chainID := getUint256FromField(rawTx, "chainId"); chainID != nil {
		tx.ChainID = chainID
	}
	if accessListRaw, ok := rawTx["accessList"].([]interface{}); ok {
		tx.AccessList = decodeAccessList(accessListRaw)
	}
	if tipCap := getUint256FromField(rawTx, "maxPriorityFeePerGas"); tipCap != nil {
		tx.TipCap = tipCap
	}
	if feeCap := getUint256FromField(rawTx, "maxFeePerGas"); feeCap != nil {
		tx.FeeCap = feeCap
	}
}

// parseCommonTx extracts the shared fields from a raw transaction into a CommonTx.
func parseCommonTx(rawTx map[string]interface{}) (*types.CommonTx, error) {
	var commonTx types.CommonTx

	nonceStr, ok := rawTx["nonce"].(string)
	if !ok {
		return nil, errors.New("missing nonce")
	}
	commonTx.Nonce = convertHexToBigInt(nonceStr).Uint64()

	gasStr, ok := rawTx["gas"].(string)
	if !ok {
		return nil, errors.New("missing gas")
	}
	commonTx.GasLimit = convertHexToBigInt(gasStr).Uint64()

	if toStr, ok := rawTx["to"].(string); ok && toStr != "" {
		addr := common.HexToAddress(toStr)
		commonTx.To = &addr
	}
	if valueStr, ok := rawTx["value"].(string); ok {
		commonTx.Value = new(uint256.Int)
		commonTx.Value.SetFromBig(convertHexToBigInt(valueStr))
	}
	if inputStr, ok := rawTx["input"].(string); ok && len(inputStr) >= 2 && inputStr[:2] == "0x" {
		commonTx.Data = common.Hex2Bytes(inputStr[2:])
	}
	if vStr, ok := rawTx["v"].(string); ok {
		commonTx.V.SetFromBig(convertHexToBigInt(vStr))
	}
	if rStr, ok := rawTx["r"].(string); ok {
		commonTx.R.SetFromBig(convertHexToBigInt(rStr))
	}
	if sStr, ok := rawTx["s"].(string); ok {
		commonTx.S.SetFromBig(convertHexToBigInt(sStr))
	}
	return &commonTx, nil
}

// decodeAccessList converts a raw access list (slice of interface{}) into types.AccessList.
func decodeAccessList(rawAccessList []interface{}) types.AccessList {
	var accessList types.AccessList
	for _, rawSlotInterface := range rawAccessList {
		slot, ok := rawSlotInterface.(map[string]interface{})
		if !ok {
			continue
		}
		tuple := types.AccessTuple{}
		if addrStr, ok := slot["address"].(string); ok {
			tuple.Address = common.HexToAddress(addrStr)
		}
		if storageKeys, ok := slot["storageKeys"].([]interface{}); ok {
			for _, keyIface := range storageKeys {
				if keyStr, ok := keyIface.(string); ok {
					tuple.StorageKeys = append(tuple.StorageKeys, common.HexToHash(keyStr))
				}
			}
		}
		accessList = append(accessList, tuple)
	}
	return accessList
}

// decodeBlobVersionedHashes converts a slice of hex strings to a slice of common.Hash.
func decodeBlobVersionedHashes(rawVersionedHashes []string) []common.Hash {
	hashes := make([]common.Hash, 0, len(rawVersionedHashes))
	for _, s := range rawVersionedHashes {
		hashes = append(hashes, common.HexToHash(s))
	}
	return hashes
}

// --- Transaction builders ---

// makeLegacyTx builds a legacy transaction.
func makeLegacyTx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    commonTx.Nonce,
			GasLimit: commonTx.GasLimit,
			To:       commonTx.To,
			Value:    commonTx.Value,
			Data:     commonTx.Data,
			V:        commonTx.V,
			R:        commonTx.R,
			S:        commonTx.S,
		},
		GasPrice: getUint256FromField(rawTx, "gasPrice"),
	}
	return tx
}

// makeAccessListTx builds an access-list transaction.
func makeAccessListTx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.AccessListTx{
		LegacyTx: types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    commonTx.Nonce,
				GasLimit: commonTx.GasLimit,
				To:       commonTx.To,
				Value:    commonTx.Value,
				Data:     commonTx.Data,
				V:        commonTx.V,
				R:        commonTx.R,
				S:        commonTx.S,
			},
			GasPrice: getUint256FromField(rawTx, "gasPrice"),
		},
	}
	if chainID := getUint256FromField(rawTx, "chainId"); chainID != nil {
		tx.ChainID = chainID
	}
	if accessListRaw, ok := rawTx["accessList"].([]interface{}); ok {
		tx.AccessList = decodeAccessList(accessListRaw)
	}
	return tx
}

// makeEip1559Tx builds an EIP-1559 dynamic fee transaction.
func makeEip1559Tx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{
		Nonce:    commonTx.Nonce,
		GasLimit: commonTx.GasLimit,
		To:       commonTx.To,
		Value:    commonTx.Value,
		Data:     commonTx.Data,
		V:        commonTx.V,
		R:        commonTx.R,
		S:        commonTx.S,
	}}
	buildDynamicFeeFields(tx, rawTx)
	return tx
}

// makeEip4844Tx builds an EIP-4844 blob transaction.
func makeEip4844Tx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	blobTx := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{
			Nonce:    commonTx.Nonce,
			GasLimit: commonTx.GasLimit,
			To:       commonTx.To,
			Value:    commonTx.Value,
			Data:     commonTx.Data,
			V:        commonTx.V,
			R:        commonTx.R,
			S:        commonTx.S,
		}},
	}
	buildDynamicFeeFields(&blobTx.DynamicFeeTransaction, rawTx)
	blobTx.MaxFeePerBlobGas = getUint256FromField(rawTx, "maxFeePerBlobGas")
	// The raw JSON is expected to contain a slice of strings.
	if rawHashes, ok := rawTx["blobVersionedHashes"].([]interface{}); ok {
		var hashStrs []string
		for _, h := range rawHashes {
			if s, ok := h.(string); ok {
				hashStrs = append(hashStrs, s)
			}
		}
		blobTx.BlobVersionedHashes = decodeBlobVersionedHashes(hashStrs)
	}
	return blobTx
}

// makeEip7702Tx builds an EIP-7702 transaction.
// (Implementation details remain to be determined.)
func makeEip7702Tx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{
			Nonce:    commonTx.Nonce,
			GasLimit: commonTx.GasLimit,
			To:       commonTx.To,
			Value:    commonTx.Value,
			Data:     commonTx.Data,
			V:        commonTx.V,
			R:        commonTx.R,
			S:        commonTx.S,
		}},
	}
	buildDynamicFeeFields(&tx.DynamicFeeTransaction, rawTx)
	if rawAuths, ok := rawTx["authorizationList"].([]interface{}); ok {
		var auths []types.Authorization
		for _, a := range rawAuths {
			if auth, ok := a.(map[string]interface{}); ok {

				cid := getUint256FromField(auth, "chainId")
				yparity := getUint256FromField(auth, "yParity")
				r := getUint256FromField(auth, "r")
				s := getUint256FromField(auth, "s")
				nonce := getUint256FromField(auth, "nonce")

				ja := types.Authorization{
					Address: common.HexToAddress(auth["address"].(string)),
				}

				ja.ChainID = *cid
				ja.YParity = uint8(yparity.Uint64())
				ja.R.SetFromBig(r.ToBig())
				ja.S.SetFromBig(s.ToBig())
				ja.Nonce = nonce.Uint64()

				auths = append(auths, ja)
			}
		}
		tx.Authorizations = auths
	}

	// TODO: Add any additional EIP-7702–specific processing here.
	return tx
}

func makeRetryableTxFunc(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumSubmitRetryableTx{}

	// Chain ID: required field (hex string)
	if chainIdHex, ok := rawTx["chainId"].(string); ok {
		tx.ChainId = convertHexToBigInt(chainIdHex)
	} else {
		// Optionally handle missing field (e.g. log or return an error)
	}

	// Request ID: expected as a hex string
	if requestIdHex, ok := rawTx["requestId"].(string); ok {
		tx.RequestId = common.HexToHash(requestIdHex)
	}

	// From: expected as a hex string address.
	if fromHex, ok := rawTx["from"].(string); ok {
		tx.From = common.HexToAddress(fromHex)
	}

	// L1BaseFee: expected as a hex string.
	if l1BaseFeeHex, ok := rawTx["l1BaseFee"].(string); ok {
		tx.L1BaseFee = convertHexToBigInt(l1BaseFeeHex)
	}

	// DepositValue: expected as a hex string.
	if depositValueHex, ok := rawTx["depositValue"].(string); ok {
		tx.DepositValue = convertHexToBigInt(depositValueHex)
	}

	// GasFeeCap: expected as a hex string.
	if gasFeeCapHex, ok := rawTx["maxFeePerGas"].(string); ok {
		tx.GasFeeCap = convertHexToBigInt(gasFeeCapHex)
	}

	// Gas limit: taken from the commonTx already parsed.
	tx.Gas = commonTx.GasLimit

	// RetryTo: expected as a hex string address. If empty, nil indicates contract creation.
	if retryToHex, ok := rawTx["retryTo"].(string); ok && retryToHex != "" {
		addr := common.HexToAddress(retryToHex)
		tx.RetryTo = &addr
	}

	// RetryValue: expected as a hex string.
	if retryValueHex, ok := rawTx["retryValue"].(string); ok {
		tx.RetryValue = convertHexToBigInt(retryValueHex)
	}

	// Beneficiary: expected as a hex string address.
	if beneficiaryHex, ok := rawTx["beneficiary"].(string); ok {
		tx.Beneficiary = common.HexToAddress(beneficiaryHex)
	}

	// MaxSubmissionFee: expected as a hex string.
	if maxSubmissionFeeHex, ok := rawTx["maxSubmissionFee"].(string); ok {
		tx.MaxSubmissionFee = convertHexToBigInt(maxSubmissionFeeHex)
	}

	// FeeRefundAddr: expected as a hex string address.
	if feeRefundAddrHex, ok := rawTx["refundTo"].(string); ok {
		tx.FeeRefundAddr = common.HexToAddress(feeRefundAddrHex)
	}

	// RetryData: expected as a hex string (with "0x" prefix) that will be decoded to bytes.
	if retryDataHex, ok := rawTx["retryData"].(string); ok && len(retryDataHex) >= 2 && retryDataHex[:2] == "0x" {
		tx.RetryData = common.Hex2Bytes(retryDataHex[2:])
	}

	return tx
}

func makeArbitrumRetryTx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumRetryTx{}

	// ChainId (expected as a hex string, e.g., "0x1")
	if chainIdHex, ok := rawTx["chainId"].(string); ok {
		tx.ChainId = convertHexToBigInt(chainIdHex)
	}

	// Nonce is taken from the common transaction fields.
	tx.Nonce = commonTx.Nonce

	// From (expected as a hex string address)
	if fromHex, ok := rawTx["from"].(string); ok {
		tx.From = common.HexToAddress(fromHex)
	}

	// GasFeeCap (expected as a hex string)
	if gasFeeCapHex, ok := rawTx["maxFeePerGas"].(string); ok {
		tx.GasFeeCap = convertHexToBigInt(gasFeeCapHex)
	}

	// Gas limit is taken from the common transaction fields.
	tx.Gas = commonTx.GasLimit

	// To is optional. A non-empty hex string is converted to an address pointer;
	// if missing or empty, nil indicates contract creation.
	if toStr, ok := rawTx["to"].(string); ok && toStr != "" {
		addr := common.HexToAddress(toStr)
		tx.To = &addr
	}

	// Value (expected as a hex string)
	if valueStr, ok := rawTx["value"].(string); ok {
		tx.Value = convertHexToBigInt(valueStr)
	}

	// Data is taken from the common transaction fields.
	tx.Data = commonTx.Data

	// TicketId (expected as a hex string)
	if ticketIdHex, ok := rawTx["ticketId"].(string); ok {
		tx.TicketId = common.HexToHash(ticketIdHex)
	}

	// RefundTo (expected as a hex string address)
	if refundToHex, ok := rawTx["refundTo"].(string); ok {
		tx.RefundTo = common.HexToAddress(refundToHex)
	}

	// MaxRefund (expected as a hex string)
	if maxRefundHex, ok := rawTx["maxRefund"].(string); ok {
		tx.MaxRefund = convertHexToBigInt(maxRefundHex)
	}

	// SubmissionFeeRefund (expected as a hex string)
	if submissionFeeRefundHex, ok := rawTx["submissionFeeRefund"].(string); ok {
		tx.SubmissionFeeRefund = convertHexToBigInt(submissionFeeRefundHex)
	}

	return tx
}

// makeArbitrumContractTx builds an ArbitrumContractTx from the common transaction fields
// and the raw JSON transaction data.
func makeArbitrumContractTx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumContractTx{}

	// ChainId (expected as a hex string, e.g. "0x1")
	if chainIdHex, ok := rawTx["chainId"].(string); ok {
		tx.ChainId = convertHexToBigInt(chainIdHex)
	}

	// RequestId (expected as a hex string)
	if requestIdHex, ok := rawTx["requestId"].(string); ok {
		tx.RequestId = common.HexToHash(requestIdHex)
	}

	// From (expected as a hex string address)
	if fromHex, ok := rawTx["from"].(string); ok {
		tx.From = common.HexToAddress(fromHex)
	}

	// GasFeeCap (expected as a hex string)
	if gasFeeCapHex, ok := rawTx["maxFeePerGas"].(string); ok {
		tx.GasFeeCap = convertHexToBigInt(gasFeeCapHex)
	}

	// Gas limit: obtained from the common transaction fields.
	tx.Gas = commonTx.GasLimit

	// To: if present and non-empty, convert to an address pointer;
	// if missing or empty, nil indicates contract creation.
	if toStr, ok := rawTx["to"].(string); ok && toStr != "" {
		addr := common.HexToAddress(toStr)
		tx.To = &addr
	}

	// Value (expected as a hex string)
	if valueStr, ok := rawTx["value"].(string); ok {
		tx.Value = convertHexToBigInt(valueStr)
	}

	// Data: taken from the common transaction fields.
	tx.Data = commonTx.Data

	return tx
}

func makeArbitrumUnsignedTx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumUnsignedTx{GasFeeCap: big.NewInt(0)}

	// ChainId: expected as a hex string (e.g., "0x1")
	if chainIdHex, ok := rawTx["chainId"].(string); ok {
		tx.ChainId = convertHexToBigInt(chainIdHex)
	}

	// From: expected as a hex string address.
	if fromHex, ok := rawTx["from"].(string); ok {
		tx.From = common.HexToAddress(fromHex)
	}

	// Nonce: already parsed and stored in commonTx.
	tx.Nonce = commonTx.Nonce

	// GasFeeCap: expected as a hex string.
	if gasFeeCapHex, ok := rawTx["maxFeePerGas"].(string); ok {
		tx.GasFeeCap = convertHexToBigInt(gasFeeCapHex)
	} else if gasFeeCapHex, ok := rawTx["gasPrice"].(string); ok {
		tx.GasFeeCap = convertHexToBigInt(gasFeeCapHex)
	}

	// Gas: taken directly from commonTx.
	tx.Gas = commonTx.GasLimit

	// To: if provided and non-empty, convert to an address pointer.
	if toStr, ok := rawTx["to"].(string); ok && toStr != "" {
		addr := common.HexToAddress(toStr)
		tx.To = &addr
	}

	// Value: expected as a hex string.
	if valueStr, ok := rawTx["value"].(string); ok {
		tx.Value = convertHexToBigInt(valueStr)
	}

	// Data: taken directly from commonTx.
	tx.Data = commonTx.Data
	return tx
}

func makeArbitrumDepositTx(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumDepositTx{}

	// ChainId: expected as a hex string (e.g., "0x1")
	if chainIdHex, ok := rawTx["chainId"].(string); ok {
		tx.ChainId = convertHexToBigInt(chainIdHex)
	}

	// L1RequestId: expected as a hex string.
	if l1RequestIdHex, ok := rawTx["requestId"].(string); ok {
		tx.L1RequestId = common.HexToHash(l1RequestIdHex)
	}

	// From: expected as a hex string address.
	if fromHex, ok := rawTx["from"].(string); ok {
		tx.From = common.HexToAddress(fromHex)
	}

	// To: expected as a hex string address.
	if toHex, ok := rawTx["to"].(string); ok {
		tx.To = common.HexToAddress(toHex)
	}

	// Value: expected as a hex string.
	if valueStr, ok := rawTx["value"].(string); ok {
		tx.Value = convertHexToBigInt(valueStr)
	}

	return tx
}

// unMarshalTransactions decodes a slice of raw transactions into types.Transactions.
func unMarshalTransactions(rawTxs []map[string]interface{}, arbitrum bool) (types.Transactions, error) {
	var txs types.Transactions

	for _, rawTx := range rawTxs {
		commonTx, err := parseCommonTx(rawTx)
		if err != nil {
			return nil, fmt.Errorf("failed to parse common fields: %w", err)
		}

		var tx types.Transaction
		// Determine the transaction type based on the "type" field.
		typeTx, ok := rawTx["type"].(string)
		if !ok {
			return nil, errors.New("missing tx type")
		}

		switch typeTx {
		case "0x0": // Legacy
			tx = makeLegacyTx(commonTx, rawTx)
		case "0x1": // Access List
			tx = makeAccessListTx(commonTx, rawTx)
		case "0x2": // EIP-1559
			tx = makeEip1559Tx(commonTx, rawTx)
		case "0x3": // EIP-4844
			tx = makeEip4844Tx(commonTx, rawTx)
		case "0x4": // EIP-7702
			tx = makeEip7702Tx(commonTx, rawTx)
		case "0x64": // ArbitrumDepositTxType
			tx = makeArbitrumDepositTx(commonTx, rawTx)
		case "0x65": // ArbitrumUnsignedTxType
			tx = makeArbitrumUnsignedTx(commonTx, rawTx)
		case "0x66": // ArbitrumContractTxType
			tx = makeArbitrumContractTx(commonTx, rawTx)
		case "0x68": // ArbitrumRetryTxType
			tx = makeArbitrumRetryTx(commonTx, rawTx)
		case "0x69": // ArbitrumSubmitRetryableTxType
			tx = makeRetryableTxFunc(commonTx, rawTx)
		case "0x6a": // ArbitrumInternalTxType
			var chainID *uint256.Int
			if chainIDOut := getUint256FromField(rawTx, "chainId"); chainIDOut != nil {
				chainID = chainIDOut
			} else {
				return nil, errors.New("missing chainId in ArbitrumInternalTxType")
			}
			tx = &types.ArbitrumInternalTx{
				Data:    commonTx.Data,
				ChainId: chainID,
			}
		case "0x78": // ArbitrumLegacyTxType
			// types.NewArbitrumLegacyTx()
			panic("imlement me")
		default:
			return nil, fmt.Errorf("unknown tx type: %s", typeTx)
		}
		txs = append(txs, tx)

	}
	return txs, nil
}

// getBlockByNumber retrieves a block via RPC, decodes it, and (if requested) verifies its hash.
func getBlockByNumber(client *rpc.Client, blockNumber *big.Int, verify bool) (*types.Block, error) {
	var block BlockJson
	err := client.CallContext(context.Background(), &block, "eth_getBlockByNumber", fmt.Sprintf("0x%x", blockNumber), true)
	if err != nil {
		return nil, err
	}

	txs, err := unMarshalTransactions(block.Transactions, verify)
	if err != nil {
		return nil, err
	}

	// Derive the TxHash from the decoded transactions.
	txHash := types.DeriveSha(txs)
	if verify && txHash != block.TxHash {
		return nil, fmt.Errorf("tx hash mismatch, expected %s, got %s. num=%d", block.TxHash, txHash, blockNumber)
	}
	blk := types.NewBlockFromNetwork(&types.Header{
		ParentHash:      block.ParentHash,
		UncleHash:       block.UncleHash,
		Coinbase:        block.Coinbase,
		Root:            block.Root,
		TxHash:          block.TxHash,
		ReceiptHash:     block.ReceiptHash,
		Bloom:           block.Bloom,
		Difficulty:      (*big.Int)(block.Difficulty),
		Number:          (*big.Int)(block.Number),
		GasLimit:        block.GasLimit.Uint64(),
		GasUsed:         block.GasUsed.Uint64(),
		Time:            block.Time.Uint64(),
		Extra:           block.Extra,
		MixDigest:       block.MixDigest,
		Nonce:           block.Nonce,
		BaseFee:         (*big.Int)(block.BaseFee),
		WithdrawalsHash: block.WithdrawalsHash,
		BlobGasUsed:     (*uint64)(block.BlobGasUsed),
		ExcessBlobGas:   (*uint64)(block.ExcessBlobGas),
		// Optional fields:
		ParentBeaconBlockRoot: block.ParentBeaconBlockRoot,
		RequestsHash:          block.RequestsHash,
	}, &types.Body{
		Transactions: txs,
		Uncles:       block.Uncles,
		Withdrawals:  block.Withdrawals,
	})
	if verify {
		if blk.Hash() != block.BlkHash {
			return nil, fmt.Errorf("block hash mismatch, expected %s, got %s. num=%d", blk.Hash(), block.BlkHash, blockNumber)
		}
	}
	return blk, nil
}

// genFromRPc connects to the RPC, fetches blocks starting from the given block,
// and writes them into the local database.
func genFromRPc(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	jsonRpcAddr := cliCtx.String(RpcAddr.Name)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	// Connect to RPC.
	client, err := rpc.Dial(jsonRpcAddr, log.Root())
	if err != nil {
		log.Warn("Error connecting to RPC", "err", err)
		return err
	}

	verification := cliCtx.Bool(Verify.Name)
	db := mdbx.MustOpen(dirs.Chaindata)
	defer db.Close()
	start := cliCtx.Uint64(FromBlock.Name)
	if start == 0 {
		var curBlock uint64
		err = db.Update(context.Background(), func(tx kv.RwTx) error {
			curBlock, err = stages.GetStageProgress(tx, stages.Bodies)
			return err
		})
		if err != nil {
			log.Warn("can't check current block", "err", err)
		}
		if curBlock == 0 {
			// write arb genesis
			log.Info("Writing arbitrum sepolia-rollup genesis")
			gen := core.GenesisBlockByChainName("sepolia-rollup")
			b := core.MustCommitGenesis(gen, db, dirs, log.New())
			log.Info("wrote arbitrum sepolia-rollup genesis", "block_hash", b.Hash().String(), "state_root", b.Root().String())
		} else {
			start = curBlock
		}
	}

	// Query latest block number.
	var latestBlockHex string
	if err := client.CallContext(context.Background(), &latestBlockHex, "eth_blockNumber"); err != nil {
		log.Warn("Error fetching latest block number", "err", err)
		return err
	}

	latestBlock := new(big.Int)
	latestBlock.SetString(latestBlockHex[2:], 16)
	noWrite := cliCtx.Bool(NoWrite.Name)

	var blockNumber big.Int
	// Process blocks from the starting block up to the latest.
	for i := start; i < latestBlock.Uint64(); {
		prev := i
		prevTime := time.Now()
		timer := time.NewTimer(40 * time.Second)
		err := db.Update(context.TODO(), func(tx kv.RwTx) error {
			for blockNum := i; blockNum < latestBlock.Uint64(); blockNum++ {
				blockNumber.SetUint64(blockNum)
				blk, err := getBlockByNumber(client, &blockNumber, verification)
				if err != nil {
					return fmt.Errorf("error fetching block %d: %w", blockNum, err)
				}
				if !noWrite {
					if err := rawdb.WriteBlock(tx, blk); err != nil {
						return fmt.Errorf("error writing block %d: %w", blockNum, err)
					}
					if err := rawdb.WriteCanonicalHash(tx, blk.Hash(), blockNum); err != nil {
						return fmt.Errorf("error writing canonical hash %d: %w", blockNum, err)
					}
					if err = rawdb.AppendCanonicalTxNums(tx, blockNum); err != nil {
						return fmt.Errorf("failed to append canonical txnum %d: %w", blockNum, err)
					}
					rawdb.WriteHeadBlockHash(tx, blk.Hash())
					if err := rawdb.WriteHeadHeaderHash(tx, blk.Hash()); err != nil {
						return err
					}
					if err := stages.SaveStageProgress(tx, stages.Headers, blockNum); err != nil {
						return err
					}
					if err := stages.SaveStageProgress(tx, stages.BlockHashes, blockNum); err != nil {
						return err
					}
					if err := stages.SaveStageProgress(tx, stages.Bodies, blockNum); err != nil {
						return err
					}
					if err := stages.SaveStageProgress(tx, stages.Senders, blockNum); err != nil {
						return err
					}

				}

				// Update the progress counter.
				i = blockNum + 1

				select {
				case <-timer.C:
					blkSec := float64(blockNum-prev) / time.Since(prevTime).Seconds()
					log.Info("Block processed", "block", blockNum, "hash", blk.Hash(), "blk/s", fmt.Sprintf("%.2f", blkSec))
					return nil
				default:
					// continue processing without waiting
				}
			}
			return nil
		})
		timer.Stop()
		if err != nil {
			log.Warn("Error updating db", "err", err)
			return err
		}
	}
	return nil
}
