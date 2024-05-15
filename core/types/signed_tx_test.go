package types_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
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
