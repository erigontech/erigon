package eth1_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	statecontracts "github.com/erigontech/erigon/core/state/contracts"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/consensus/merge"
	eth1utils "github.com/erigontech/erigon/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestInvalidNewPayloadReorgDoesNotAffectCanonicalFcu(t *testing.T) {
	ctx := t.Context()
	mockSentry := mock.MockWithTxPoolOsaka(t)
	chainConfig := mockSentry.ChainConfig
	bankPrivKey := mockSentry.Key // we have 1 eth there
	logger := mockSentry.Log
	exec := mockSentry.Eth1ExecutionService
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)
	_, grpcCon := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(grpcCon), func() {}, mockSentry.Log)
	baseJsonRpc := jsonrpc.NewBaseApi(ff, kvcache.NewDummy(), mockSentry.BlockReader, false, rpccfg.DefaultEvmCallTimeout, mockSentry.Engine, mockSentry.Dirs, nil)
	ethApi := jsonrpc.NewEthAPI(baseJsonRpc, mockSentry.DB, nil, txPool, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, logger)
	cb := contracts.NewDirectBackend(ethApi)
	genesisHeaderNum := uint64(0)
	genesisHeaderResp, err := exec.GetHeader(ctx, &executionproto.GetSegmentRequest{BlockNumber: &genesisHeaderNum})
	require.NoError(t, err)
	genesis, err := eth1utils.HeaderRpcToHeader(genesisHeaderResp.Header)
	require.NoError(t, err)

	txnOpts, err := bind.NewKeyedTransactorWithChainID(bankPrivKey, chainConfig.ChainID)
	require.NoError(t, err)
	_, _, _, err = statecontracts.DeployPoint3dFactory(txnOpts, cb)
	require.NoError(t, err)

	canonical1ParentBeaconBlockRoot := common.BigToHash(big.NewInt(999_999_999))
	reqAssembleCanonical1 := &executionproto.AssembleBlockRequest{
		ParentHash:            gointerfaces.ConvertHashToH256(genesis.Hash()),
		Timestamp:             genesis.Time + 12,
		PrevRandao:            gointerfaces.ConvertHashToH256(common.BigToHash(big.NewInt(1))),
		ParentBeaconBlockRoot: gointerfaces.ConvertHashToH256(canonical1ParentBeaconBlockRoot),
		SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(genesis.Coinbase),
		Withdrawals:           []*typesproto.Withdrawal{},
	}

	respAssembleCanonical1, err := exec.AssembleBlock(ctx, reqAssembleCanonical1)
	require.NoError(t, err)
	require.False(t, respAssembleCanonical1.Busy)

	respCanonical1, err := exec.GetAssembledBlock(ctx, &executionproto.GetAssembledBlockRequest{Id: respAssembleCanonical1.Id})
	require.NoError(t, err)
	require.False(t, respCanonical1.Busy)
	canonicalPayload1 := respCanonical1.Data.ExecutionPayload
	canonicalWithdrawalsHash1 := types.DeriveSha(types.Withdrawals(eth1utils.ConvertWithdrawalsFromRpc(canonicalPayload1.Withdrawals)))
	canonicalRequestHash1 := empty.RequestsHash
	if respCanonical1.Data.Requests != nil {
		canonicalRequests1 := make(types.FlatRequests, 0)
		lastReqType := -1
		for i, r := range respCanonical1.Data.Requests.Requests {
			if len(r) <= 1 || lastReqType >= 0 && int(r[0]) <= lastReqType {
				panic(fmt.Sprintf("Invalid Request at index %d", i))
			}
			lastReqType = int(r[0])
			canonicalRequests1 = append(canonicalRequests1, types.FlatRequest{Type: r[0], RequestData: r[1:]})
		}
		canonicalRequestHash1 = *canonicalRequests1.Hash()
	}
	canonicalHeader1 := &types.Header{
		ParentHash:            gointerfaces.ConvertH256ToHash(canonicalPayload1.ParentHash),
		Coinbase:              gointerfaces.ConvertH160toAddress(canonicalPayload1.Coinbase),
		Root:                  gointerfaces.ConvertH256ToHash(canonicalPayload1.StateRoot),
		TxHash:                types.DeriveSha(types.BinaryTransactions(canonicalPayload1.Transactions)),
		ReceiptHash:           gointerfaces.ConvertH256ToHash(canonicalPayload1.ReceiptRoot),
		Bloom:                 gointerfaces.ConvertH2048ToBloom(canonicalPayload1.LogsBloom),
		Difficulty:            merge.ProofOfStakeDifficulty,
		Number:                big.NewInt(1),
		GasLimit:              canonicalPayload1.GasLimit,
		GasUsed:               canonicalPayload1.GasUsed,
		Time:                  canonicalPayload1.Timestamp,
		Extra:                 canonicalPayload1.ExtraData,
		MixDigest:             gointerfaces.ConvertH256ToHash(canonicalPayload1.PrevRandao),
		Nonce:                 merge.ProofOfStakeNonce,
		BaseFee:               gointerfaces.ConvertH256ToUint256Int(canonicalPayload1.BaseFeePerGas).ToBig(),
		WithdrawalsHash:       &canonicalWithdrawalsHash1,
		BlobGasUsed:           canonicalPayload1.BlobGasUsed,
		ExcessBlobGas:         canonicalPayload1.ExcessBlobGas,
		ParentBeaconBlockRoot: &canonical1ParentBeaconBlockRoot,
		RequestsHash:          &canonicalRequestHash1,
	}
	canonicalDecodedTxns1, err := types.DecodeTransactions(canonicalPayload1.Transactions)
	require.NoError(t, err)
	canonicalBlock1 := types.NewBlock(
		canonicalHeader1,
		canonicalDecodedTxns1,
		nil,
		nil,
		eth1utils.ConvertWithdrawalsFromRpc(canonicalPayload1.Withdrawals),
	)

	insertionResult, err := exec.InsertBlocks(ctx, &executionproto.InsertBlocksRequest{
		Blocks: []*executionproto.Block{
			eth1utils.ConvertBlockToRPC(canonicalBlock1),
		},
	})
	require.NoError(t, err)
	require.Equal(t, executionproto.ExecutionStatus_Success, insertionResult.Result)

	fcuReceipt, err := exec.UpdateForkChoice(ctx, &executionproto.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(canonicalBlock1.Hash()),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(genesis.Hash()),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(genesis.Hash()),
		Timeout:            0,
	})
	require.NoError(t, err)
	require.Equal(t, executionproto.ExecutionStatus_Success, fcuReceipt.Status)
	require.Equal(t, "", fcuReceipt.ValidationError)
	require.Equal(t, canonicalBlock1.Hash(), common.Hash(gointerfaces.ConvertH256ToHash(fcuReceipt.LatestValidHash)))
}
