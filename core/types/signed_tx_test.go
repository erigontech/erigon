package types_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

func TestJsonToExecPayload(t *testing.T) {
	payloadb, err := os.ReadFile("testdata/execution_payload.json")
	if err != nil {
		t.Fatalf("could not read file: %v", err)
	}

	//payload := string(payloadb)
	exp := engine_types.ExecutionPayload{}
	if err = json.Unmarshal(payloadb, &exp); err != nil {
		t.Fatalf("could not unmarshal json: %v", err)
	}

	var chainConfig *chain.Config
	if cConf, _, err1 := tests.GetChainConfig("Cancun"); err1 != nil {
		t.Fatalf("could not get chain config: %v", err1)
	} else { //nolint:golint
		chainConfig = cConf
	}

	signer := *types.MakeSigner(chainConfig, exp.BlockNumber.Uint64(), uint64(exp.Timestamp))
	txs := [][]byte{}
	for _, transaction := range exp.Transactions {
		txs = append(txs, transaction.Inner)
	}

	txsd, err := types.DecodeTransactionsJson(signer, txs)
	if err != nil {
		t.Fatalf("could not decode transactions: %v", err)
	}

	t.Logf("decoded transactions: %v", txsd)
}

func TestUnmarshalExecPaylaod(t *testing.T) {
	type ExecutionPayload2 struct {
		WithdrawalRequests types.WithdrawalRequests `json:"withdrawalRequests"`
	}
	ep := &ExecutionPayload2{}
	jsons := "{\"withdrawalRequests\":[]}"
	if err := json.Unmarshal([]byte(jsons), ep); err != nil {
		t.Fatalf("could not unmarshal json: %v", err)
	}

	if ep.WithdrawalRequests == nil {
		t.Fatalf("wrong unmarshal")
	}
}

func TestUnmarshalExecPaylaod2(t *testing.T) {
	ep := &engine_types.ExecutionPayload{}
	jsons := " {       \"baseFeePerGas\": \"0xd4b481\",       \"blobGasUsed\": \"0x0\",       \"blockHash\": \"0x34b5ed2606b583ef8c42d1452aada87fbf2cd6f1039dfd56ae2b1bced2a7ffff\",       \"blockNumber\": \"0x20\",       \"depositRequests\": [],       \"excessBlobGas\": \"0x0\",       \"extraData\": \"0x\",       \"feeRecipient\": \"0x8943545177806ed17b9f23f0a21ee5948ecaa776\",       \"gasLimit\": \"0x1899276\",       \"gasUsed\": \"0x0\",       \"logsBloom\": \"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",       \"parentHash\": \"0x1f86a226c3551b4d476e6bef9ff96853d8cb2d25e99989365c03ae62878b2fcf\",       \"prevRandao\": \"0xf5fc1c90afe2b0190f129e10bbed63522373338fff9d7cb9b1947d636566c05d\",       \"receiptsRoot\": \"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421\",       \"stateRoot\": \"0x736ea7633a911b465c32d940ba9a5d78e7796ffbd09894edd1036827b39431db\",       \"timestamp\": \"0x664717e7\",       \"transactions\": [],       \"withdrawalRequests\": [],       \"withdrawals\": [] }"
	if err := json.Unmarshal([]byte(jsons), ep); err != nil {
		t.Fatalf("could not unmarshal json: %v", err)
	}

	if ep.WithdrawalRequests == nil {
		t.Fatalf("wrong unmarshal")
	}
}

func TestUnma3(t *testing.T) {
	//txs := make([]types.SignedTransaction, 0)
	ssz_txs := solid.NewDynamicListSSZ[*types.SignedTransaction](1 << 20)
	hash, err := ssz_txs.HashSSZ()
	if err != nil {
		fmt.Println("ssz_txs_hash", err)
		return
	}
	s := hex.EncodeToString(hash[:])
	fmt.Println("ssz_txs_hash", s)

}
