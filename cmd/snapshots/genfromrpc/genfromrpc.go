package genfromrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	turbocli "github.com/erigontech/erigon/turbo/cli"
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
	Action: func(cliCtx *cli.Context) error { return genFromRPc(cliCtx) },
	Name:   "genfromrpc",
	Usage:  "genfromrpc utilities",
	Flags: []cli.Flag{
		&utils.DataDirFlag, &RpcAddr, &Verify, &FromBlock, &Arbitrum,
		&turbocli.L2RPCReceiptAddrFlag,
		&turbocli.L2RPCBlockRPSFlag, &turbocli.L2RPCBlockBurstFlag,
		&turbocli.L2RPCReceiptRPSFlag, &turbocli.L2RPCReceiptBurstFlag,
	},
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

// ReceiptJson holds the minimal receipt data needed for timeboosted transactions
type ReceiptJson struct {
	Status          hexutil.Uint64 `json:"status"`
	Type            string         `json:"type"`
	TransactionHash common.Hash    `json:"transactionHash"`
	Timeboosted     *bool          `json:"timeboosted,omitempty"`
	GasUsed         *hexutil.Big   `json:"gasUsed,omitempty"`
}

// BlockMetadataJson holds the response from arb_getRawBlockMetadata
type BlockMetadataJson struct {
	BlockNumber uint64        `json:"blockNumber"`
	RawMetadata hexutil.Bytes `json:"rawMetadata"`
}

// IsTxTimeboosted returns whether the tx at txIndex was timeboosted based on the raw metadata.
// The first byte is version, remaining bytes are a bitmask where bit N = 1 means tx N is timeboosted.
func IsTxTimeboosted(rawMetadata []byte, txIndex int) *bool {
	if len(rawMetadata) == 0 || txIndex < 0 {
		return nil
	}
	maxTxCount := (len(rawMetadata) - 1) * 8
	if txIndex >= maxTxCount {
		return nil
	}
	result := rawMetadata[1+(txIndex/8)]&(1<<(txIndex%8)) != 0
	return &result
}

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

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "429") || strings.Contains(strings.ToLower(errStr), "internal server")
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

func makeArbitrumLegacyTxFunc(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumLegacyTxData{LegacyTx: &types.LegacyTx{CommonTx: *commonTx}}

	if gasPriceHex, ok := rawTx["gasPrice"].(string); ok {
		tx.GasPrice = uint256.MustFromHex(gasPriceHex)
	}
	if l1BlockNum, ok := rawTx["l1BlockNumber"].(string); ok {
		tx.L1BlockNumber = convertHexToBigInt(l1BlockNum).Uint64()
		// } else {
		// 	if l1BlockNum, ok := rawTx["blockNumber"].(string); ok {
		// 		tx.L1BlockNumber = convertHexToBigInt(l1BlockNum).Uint64()
		// 	}
	}
	if effectiveGasPrice, ok := rawTx["effectiveGasPrice"].(string); ok {
		tx.EffectiveGasPrice = convertHexToBigInt(effectiveGasPrice).Uint64()
	}
	// if hashOverride, ok := rawTx["hashOverride"].(string); ok {
	// 	tx.HashOverride = common.HexToHash(hashOverride)
	// }
	if hashOverride, ok := rawTx["hash"].(string); ok {
		tx.HashOverride = common.HexToHash(hashOverride)
	}
	sender, ok := commonTx.GetSender()
	if ok {
		tx.OverrideSender = &sender
	}

	// return types.NewArbitrumLegacyTx(&types.LegacyTx{CommonTx: *commonTx, GasPrice: tx.GasPrice}, tx.HashOverride, tx.EffectiveGasPrice, tx.L1BlockNumber, tx.OverrideSender)
	return tx
}

func makeRetryableTxFunc(commonTx *types.CommonTx, rawTx map[string]interface{}) types.Transaction {
	tx := &types.ArbitrumSubmitRetryableTx{}

	// Chain ID: required field (hex string)
	if chainIdHex, ok := rawTx["chainId"].(string); ok {
		tx.ChainId = convertHexToBigInt(chainIdHex)
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

// Transaction types that can have the timeboosted flag set
// - LegacyTx
// - AccessListTx
// - DynamicFeeTx
// - SetCodeTx
// - BlobTx
// - ArbitrumRetryTx
var timeboostedTxTypes = map[string]bool{
	"0x0":  true,
	"0x1":  true,
	"0x2":  true,
	"0x3":  true,
	"0x4":  true,
	"0x68": true,
}

// TimeboostBlock returns the block number at which timeboost was activated for
// the given chain ID. Returns 0 for unknown chains (timeboost disabled).
func TimeboostBlock(chainID uint64) uint64 {
	switch chainID {
	case 42161: // Arbitrum One
		return 327_000_000
	case 421614: // Arbitrum Sepolia
		return 45_000_000
	default:
		return 0
	}
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
	isArbitrum := cliCtx.Bool(Arbitrum.Name)

	receiptRpcAddr := cliCtx.String(turbocli.L2RPCReceiptAddrFlag.Name)
	var receiptClient *rpc.Client
	if isArbitrum && receiptRpcAddr != "" {
		receiptClient, err = rpc.Dial(receiptRpcAddr, log.Root())
		if err != nil {
			log.Warn("Error connecting to receipt RPC", "err", err, "url", receiptRpcAddr)
			return err
		}
		log.Info("Connected to receipt RPC", "url", receiptRpcAddr)
	}

	db := mdbx.MustOpen(dirs.Chaindata)
	defer db.Close()
	var start uint64
	if from := cliCtx.Uint64(FromBlock.Name); from > 0 {
		start = from
	} else {
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
			// log.Info("Writing arbitrum sepolia-rollup genesis")

			// gen := chain.ArbSepoliaRollupGenesisBlock()

			// b := core.MustCommitGenesis(gen, db, dirs, log.New())
			// log.Info("wrote arbitrum sepolia-rollup genesis", "block_hash", b.Hash().String(), "state_root", b.Root().String())
		} else {
			start = curBlock + 1
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

	var timeboostBlock uint64
	if isArbitrum {
		var chainIdHex string
		if err := client.CallContext(context.Background(), &chainIdHex, "eth_chainId"); err != nil {
			return fmt.Errorf("failed to query chain ID: %w", err)
		}
		chainID := new(big.Int)
		chainID.SetString(chainIdHex[2:], 16)
		timeboostBlock = TimeboostBlock(chainID.Uint64())
		log.Info("Timeboost activation", "chainId", chainID, "block", timeboostBlock)
	}

	blockRPS := cliCtx.Int(turbocli.L2RPCBlockRPSFlag.Name)
	blockBurst := cliCtx.Int(turbocli.L2RPCBlockBurstFlag.Name)
	receiptRPS := cliCtx.Int(turbocli.L2RPCReceiptRPSFlag.Name)
	receiptBurst := cliCtx.Int(turbocli.L2RPCReceiptBurstFlag.Name)

	_, err = GetAndCommitBlocks(context.Background(), db, nil, client, receiptClient, start, latestBlock.Uint64(), verification, isArbitrum, noWrite, nil, timeboostBlock, blockRPS, blockBurst, receiptRPS, receiptBurst)
	return err
}

var (
	receiptQueries  = new(atomic.Uint64)
	prevReceiptTime = new(atomic.Uint64)
)

func GetAndCommitBlocks(ctx context.Context, db kv.RwDB, rwTx kv.RwTx, client, receiptClient *rpc.Client, startBlockNum, endBlockNum uint64, verify, isArbitrum, dryRun bool, f func(tx kv.RwTx, lastBlockNum uint64) error, timeboostBlock uint64, blockRPS, blockBurst, receiptRPS, receiptBurst int) (lastBlockNum uint64, err error) {
	var (
		batchSize = uint64(5)

		logInterval   = time.Second * 40
		logEvery      = time.NewTicker(logInterval)
		lastBlockHash common.Hash
		totalBlocks   = endBlockNum - startBlockNum
	)

	defer logEvery.Stop()

	if receiptClient != nil {
		receiptClient.SetRequestLimit(rate.Limit(receiptRPS), receiptBurst)
	}
	client.SetRequestLimit(rate.Limit(blockRPS), blockBurst)

	for prev := startBlockNum; prev < endBlockNum; {
		blocks, err := FetchBlocksBatch(client, receiptClient, prev, endBlockNum, batchSize, verify, isArbitrum, timeboostBlock)
		if err != nil {
			log.Warn("Error fetching block batch", "startBlockNum", prev, "err", err)
			return lastBlockNum, err
		}
		if len(blocks) == 0 {
			log.Info("No more blocks fetched, exiting", "latestFetchedBlock", lastBlockNum, "hash", lastBlockHash)
		}

		last := blocks[len(blocks)-1]
		lastBlockNum = last.NumberU64()
		prev = lastBlockNum + 1
		lastBlockHash = last.Hash()

		select {
		case <-logEvery.C:
			blkSec := float64(prev-startBlockNum) / logInterval.Seconds()
			log.Info("Progress", "block", prev-1,
				"blocks ahead", common.PrettyCounter(endBlockNum-prev), "done%", fmt.Sprintf("%.2f", (1-(float64(endBlockNum-prev)/float64(totalBlocks)))*100),
				"hash", lastBlockHash, "blk/s", fmt.Sprintf("%.2f", blkSec))
			startBlockNum = prev

			prevReceiptTime.Store(uint64(time.Now().Unix()))
			receiptQueries.Store(0)
		default:
		}

		if dryRun {
			continue
		}

		if rwTx != nil {
			err = commitUpdate(rwTx, blocks)
			if err != nil {
				return 0, err
			}
			if f != nil {
				err = f(rwTx, lastBlockNum)
			}

		} else {
			err = db.Update(ctx, func(tx kv.RwTx) error {
				if err := commitUpdate(tx, blocks); err != nil {
					return err
				}
				if f != nil {
					err = f(tx, lastBlockNum)
				}
				return err
			})
		}
		if err != nil {
			return 0, err
		}
	}
	return lastBlockNum, nil
}

func commitUpdate(tx kv.RwTx, blocks []*types.Block) error {
	var latest *types.Block
	var blockNum uint64
	var firstBlockNum uint64
	for _, blk := range blocks {
		blockNum = blk.NumberU64()
		if firstBlockNum == 0 {
			firstBlockNum = blockNum
		}

		//if err := rawdb.WriteBlock(tx, blk); err != nil {
		if err := rawdb.WriteHeader(tx, blk.Header()); err != nil {
			return fmt.Errorf("error writing block %d: %w", blockNum, err)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(tx, blk.Hash(), blockNum, blk.RawBody()); err != nil {
			return fmt.Errorf("cannot write body: %s", err)
		}

		td := new(big.Int).Set(blk.Difficulty())
		if blockNum > 0 {
			parentTd, err := rawdb.ReadTd(tx, blk.Header().ParentHash, blockNum-1)
			if err != nil || parentTd == nil {
				return fmt.Errorf("failed to read parent total difficulty for block %d: %w", blockNum, err)
			}
			td.Add(td, parentTd)
		}
		if err := rawdb.WriteTd(tx, blk.Hash(), blockNum, td); err != nil {
			return fmt.Errorf("failed to write total difficulty %d: %w", blockNum, err)
		}

		if err := rawdb.WriteCanonicalHash(tx, blk.Hash(), blockNum); err != nil {
			return fmt.Errorf("error writing canonical hash %d: %w", blockNum, err)
		}

		latest = blk
		rawdb.WriteHeadBlockHash(tx, latest.Hash())
		if err := rawdb.WriteHeadHeaderHash(tx, latest.Hash()); err != nil {
			return err
		}
	}

	if latest != nil {
		if err := rawdbv3.TxNums.Truncate(tx, firstBlockNum); err != nil {
			return err
		}
		if err := rawdb.AppendCanonicalTxNums(tx, firstBlockNum); err != nil {
			return err
		}

		syncStages := []stages.SyncStage{
			stages.Headers, // updated by  cfg.bodyDownload.UpdateFromDb(tx);
			stages.Bodies,
			stages.BlockHashes,
			stages.Senders,
		}
		for _, stage := range syncStages {
			if err := stages.SaveStageProgress(tx, stage, blockNum); err != nil {
				return fmt.Errorf("failed to save stage progress for stage %q at block %d: %w", stage, blockNum, err)
			}
		}
	}
	return nil
}

// FetchBlockMetadataBatch fetches raw block metadata for a range of blocks using arb_getRawBlockMetadata
func FetchBlockMetadataBatch(ctx context.Context, client *rpc.Client, startBlock, endBlock uint64) (map[uint64][]byte, error) {
	if client == nil {
		return nil, nil
	}

	var result []BlockMetadataJson
	err := client.CallContext(ctx, &result, "arb_getRawBlockMetadata",
		fmt.Sprintf("0x%x", startBlock),
		fmt.Sprintf("0x%x", endBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block metadata for range %d-%d: %w", startBlock, endBlock, err)
	}

	metadataMap := make(map[uint64][]byte, len(result))
	for _, item := range result {
		metadataMap[item.BlockNumber] = item.RawMetadata
	}
	return metadataMap, nil
}

// GetBlockByNumber retrieves a block via RPC, decodes it, and (if requested) verifies its hash.
// timeboostBlock is the block number at which timeboost was activated (0 = disabled).
func GetBlockByNumber(ctx context.Context, client, receiptClient *rpc.Client, blockNumber *big.Int, verify, isArbitrum bool, blockMetadata []byte, timeboostBlock uint64) (*types.Block, error) {
	var block BlockJson
	err := client.CallContext(ctx, &block, "eth_getBlockByNumber", fmt.Sprintf("0x%x", blockNumber), true)
	if err != nil {
		return nil, err
	}

	timeboostActivated := timeboostBlock > 0 && block.Number.Uint64() > timeboostBlock
	txs, err := unMarshalTransactions(ctx, receiptClient, block.Transactions, verify, isArbitrum, blockMetadata, timeboostActivated)
	if err != nil {
		return nil, err
	}

	// Derive the TxHash from the decoded transactions.
	txHash := types.DeriveSha(txs)
	if verify && txHash != block.TxHash {
		log.Error("transactionHash mismatch", "expected", block.TxHash, "got", txHash, "num", blockNumber)
		for i, tx := range txs {
			log.Error("tx", "index", i, "hash", tx.Hash(), "type", tx.Type())
		}
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

func unMarshalTransactions(ctx context.Context, client *rpc.Client, rawTxs []map[string]interface{}, verify bool, isArbitrum bool, blockMetadata []byte, timeboostActivated bool) (types.Transactions, error) {
	txs := make(types.Transactions, len(rawTxs))

	receiptsEnabled := client != nil
	var unmarshalWg errgroup.Group

	for i, rawTx := range rawTxs {
		idx := i
		txData := rawTx
		unmarshalWg.Go(func() error {
			commonTx, err := parseCommonTx(txData)
			if err != nil {
				return fmt.Errorf("failed to parse common fields at index %d: %w", idx, err)
			}

			typeTx, ok := txData["type"].(string)
			if !ok {
				return fmt.Errorf("missing tx type at index %d", idx)
			}
			var tx types.Transaction
			switch typeTx {
			case "0x0": // Legacy
				tx = makeLegacyTx(commonTx, txData)
			case "0x1": // Access List
				tx = makeAccessListTx(commonTx, txData)
			case "0x2": // EIP-1559
				tx = makeEip1559Tx(commonTx, txData)
			case "0x3": // EIP-4844
				tx = makeEip4844Tx(commonTx, txData)
			case "0x4": // EIP-7702
				tx = makeEip7702Tx(commonTx, txData)
			case "0x64": // ArbitrumDepositTxType
				tx = makeArbitrumDepositTx(commonTx, txData)
			case "0x65": // ArbitrumUnsignedTxType
				tx = makeArbitrumUnsignedTx(commonTx, txData)
			case "0x66": // ArbitrumContractTxType
				tx = makeArbitrumContractTx(commonTx, txData)
			case "0x68": // ArbitrumRetryTxType
				tx = makeArbitrumRetryTx(commonTx, txData)
			case "0x69": // ArbitrumSubmitRetryableTxType
				tx = makeRetryableTxFunc(commonTx, txData)
			case "0x6a": // ArbitrumInternalTxType
				var chainID *uint256.Int
				if chainIDOut := getUint256FromField(txData, "chainId"); chainIDOut != nil {
					chainID = chainIDOut
				} else {
					return fmt.Errorf("missing chainId in ArbitrumInternalTxType at index %d", idx)
				}
				tx = &types.ArbitrumInternalTx{
					Data:    commonTx.Data,
					ChainId: chainID,
				}
			case "0x78": // ArbitrumLegacyTxType
				tx = makeArbitrumLegacyTxFunc(commonTx, txData)
			default:
				return fmt.Errorf("unknown tx type: %s at index %d", typeTx, idx)
			}

			// Set timeboosted from block metadata if available
			if timeboostActivated && len(blockMetadata) > 0 && timeboostedTxTypes[typeTx] {
				timeboosted := IsTxTimeboosted(blockMetadata, idx)
				if timeboosted != nil {
					tx.SetTimeboosted(timeboosted)
				}
			}

			// For ArbitrumSubmitRetryableTxType, we still need to fetch receipt to get EffectiveGasUsed
			if receiptsEnabled && tx.Type() == types.ArbitrumSubmitRetryableTxType {
				if txData["hash"] == "" {
					return errors.New("missing tx hash for receipt fetch")
				}

				receiptQueries.Add(1)

				maxRetries := 4
				backoff := time.Millisecond * 150

				var receipt ReceiptJson
				for attempt := 0; attempt < maxRetries; attempt++ {
					err = client.CallContext(ctx, &receipt, "eth_getTransactionReceipt", txData["hash"])
					if err == nil {
						if tx.Hash() != receipt.TransactionHash {
							receipt = ReceiptJson{}
							continue
						}
						break
					}
					if !isRetryableError(err) {
						break
					}

					receipt = ReceiptJson{}

					if attempt < maxRetries-1 {
						time.Sleep(backoff)
						backoff *= 2
					}
				}

				if err != nil {
					log.Info("receipt queries", "total", receiptQueries.Load())
					return fmt.Errorf("failed to get receipt for tx %s after %d attempts: %w", txData["hash"], maxRetries, err)
				}
				if receipt.TransactionHash != (common.Hash{}) {
					//if tx.Hash() != receipt.TransactionHash {
					//	log.Error("fetched receipt tx hash mismatch", "expected", txData["hash"],
					//		"got", receipt.TransactionHash, "txIndex", idx,
					//		"receipt", fmt.Sprintf("%+v", receipt))
					//	return fmt.Errorf("receipt tx hash mismatch for tx %s", txData["hash"])
					//}
					// can use receipts if blockMetadata is not available
					//if receipt.Timeboosted != nil {
					//	tx.SetTimeboosted(receipt.Timeboosted)
					//}


					if egu := receipt.GasUsed; egu != nil && egu.Uint64() > 0 {
						if srtx, ok := tx.(*types.ArbitrumSubmitRetryableTx); ok {
							srtx.EffectiveGasUsed = egu.Uint64()
							tx = srtx
						}
					}
				}
			}

			txs[idx] = tx

			return nil
		})
	}

	if err := unmarshalWg.Wait(); err != nil {
		return nil, err
	}
	return txs, nil
}

// FetchBlocksBatch fetches multiple blocks concurrently and returns them sorted by block number
func FetchBlocksBatch(client, receiptClient *rpc.Client, startBlock, endBlock, batchSize uint64, verify, isArbitrum bool, timeboostBlock uint64) ([]*types.Block, error) {
	if startBlock >= endBlock {
		return nil, nil
	}

	actualBatchSize := batchSize
	if endBlock-startBlock < batchSize {
		actualBatchSize = endBlock - startBlock
	}

	// Fetch block metadata for the batch in one RPC call
	var metadataMap map[uint64][]byte
	var err error
	metadataMap, err = FetchBlockMetadataBatch(context.Background(), client, startBlock, startBlock+actualBatchSize)
	if err != nil {
		log.Crit("Failed to fetch block metadata batch", "err", err)
	}

	blocks := make([]*types.Block, actualBatchSize)
	var eg errgroup.Group

	for i := uint64(0); i < actualBatchSize; i++ {
		idx := i
		blockNum := startBlock + i
		blockMetadata := metadataMap[blockNum]

		eg.Go(func() error {
			blockNumber := new(big.Int).SetUint64(blockNum)
			blk, err := GetBlockByNumber(context.Background(), client, receiptClient, blockNumber, verify, isArbitrum, blockMetadata, timeboostBlock)
			if err != nil {
				return fmt.Errorf("error fetching block %d: %w", blockNum, err)
			}
			blocks[idx] = blk
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return blocks, nil
}
