package cltypes

import (
	"encoding/json"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

func TestMarshalDenebBeaconBlock(t *testing.T) {
	block := NewDenebBeaconBlock(&clparams.MainnetBeaconConfig)
	data, err := json.Marshal(block)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}
	t.Logf("MarshalJSON: %s", data)
	// unmarshal
	block2 := NewDenebBeaconBlock(&clparams.MainnetBeaconConfig)
	err = json.Unmarshal(data, block2)
	if err != nil {
		t.Fatalf("UnmarshalJSON failed: %v", err)
	}
	if block.Block != block2.Block {
		t.Fatalf("Block mismatch: %v != %v", block.Block, block2.Block)
	}
}
