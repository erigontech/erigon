package engineapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// same cycle geth uses: mean tx ~700 B, what 30-40 Mgas mainnet blocks carry
var benchTxDataSizes = []int{0, 68, 132, 356, 900, 2500}

func makeBenchNewPayloadRequest(b *testing.B, numTx int) []byte {
	b.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	to := common.Address{0x11}

	txs := make([]hexutil.Bytes, numTx)
	for i := range txs {
		size := benchTxDataSizes[i%len(benchTxDataSizes)]
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i + j)
		}
		txn := types.MustSignNewTx(key, *signer, &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    uint64(i),
				GasLimit: 21000 + uint64(size)*16,
				To:       &to,
				Value:    *uint256.NewInt(1),
				Data:     data,
			},
			ChainID: *uint256.NewInt(1),
			TipCap:  *uint256.NewInt(1_000_000_000),
			FeeCap:  *uint256.NewInt(10_000_000_000),
		})
		var buf bytes.Buffer
		if err := txn.MarshalBinary(&buf); err != nil {
			b.Fatal(err)
		}
		txs[i] = buf.Bytes()
	}

	withdrawals := make([]*types.Withdrawal, 16)
	for i := range withdrawals {
		withdrawals[i] = &types.Withdrawal{Index: uint64(i), Validator: uint64(i), Address: to, Amount: 1e9}
	}

	zero := hexutil.Uint64(0)
	payload := &engine_types.ExecutionPayload{
		ParentHash:    common.Hash{0x01},
		FeeRecipient:  to,
		StateRoot:     common.Hash{0x02},
		ReceiptsRoot:  common.Hash{0x03},
		LogsBloom:     make([]byte, 256),
		PrevRandao:    common.Hash{0x04},
		BlockNumber:   1,
		GasLimit:      60_000_000,
		GasUsed:       30_000_000,
		Timestamp:     1700000000,
		ExtraData:     []byte("benchmark"),
		BaseFeePerGas: (*hexutil.Big)(big.NewInt(1_000_000_000)),
		BlockHash:     common.Hash{0x05},
		Transactions:  txs,
		Withdrawals:   withdrawals,
		BlobGasUsed:   &zero,
		ExcessBlobGas: &zero,
	}
	enc, err := json.Marshal(payload)
	if err != nil {
		b.Fatal(err)
	}
	root, err := json.Marshal(common.Hash{0x42})
	if err != nil {
		b.Fatal(err)
	}
	return []byte(fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"engine_newPayloadV4","params":[%s,[],%s,[]]}`, enc, root))
}

type npStub struct{ n int }

func (s *npStub) NewPayloadV4(ctx context.Context, payload *engine_types.ExecutionPayload,
	expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes) (*engine_types.PayloadStatus, error) {
	s.n = len(payload.Transactions)
	return &engine_types.PayloadStatus{Status: engine_types.ValidStatus}, nil
}

func BenchmarkNewPayloadDecode(b *testing.B) {
	for _, numTx := range []int{64, 192, 384} {
		req := makeBenchNewPayloadRequest(b, numTx)
		for _, stream := range []bool{true, false} {
			label := "stream"
			if !stream {
				label = "nostream"
			}
			b.Run(fmt.Sprintf("txs=%d/kb=%d/%s", numTx, len(req)/1024, label), func(b *testing.B) {
				stub := &npStub{}
				srv := rpc.NewServer(50, false, false, !stream, log.New(), 0)
				if err := srv.RegisterName("engine", stub); err != nil {
					b.Fatal(err)
				}
				defer srv.Stop()
				body := string(req)
				b.ReportAllocs()
				b.SetBytes(int64(len(req)))
				for b.Loop() {
					r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
					r.Header.Set("content-type", "application/json")
					w := httptest.NewRecorder()
					srv.ServeHTTP(w, r)
					if w.Code != http.StatusOK {
						b.Fatalf("status %d: %s", w.Code, w.Body.String())
					}
				}
				b.StopTimer()
				if stub.n != numTx {
					b.Fatalf("stub saw %d txs, want %d", stub.n, numTx)
				}
			})
		}
	}
}
