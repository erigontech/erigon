package cltypes

import (
	"encoding/json"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func TestValidatorRegistration(t *testing.T) {
	vr := &ValidatorRegistration{
		Message: struct {
			FeeRecipient libcommon.Address `json:"fee_recipient"`
			GasLimit     string            `json:"gas_limit"`
			Timestamp    string            `json:"timestamp"`
			PubKey       libcommon.Bytes48 `json:"pubkey"`
		}{
			FeeRecipient: libcommon.BytesToAddress([]byte("0xAbcF8e0d4e9587369b2301D0790347320302cc09")),
			GasLimit:     "0",
			Timestamp:    "0",
			PubKey:       libcommon.Bytes48([]byte("0x93247f2209abcacf57b75a51dafae777f9dd38bc7053d1af526f220a7489a6d3a2753e5f3e8b1cfe39b56f43611df74a")),
		},
		Signature: libcommon.Bytes96([]byte("0x1b66ac1fb663c9bc59509846d6ec05345bd908eda73e670af888da41af171505cc411d61252fb6cb3fa0017b679f8bb2305b26a285fa2737f175668d0dff91cc1b66ac1fb663c9bc59509846d6ec05345bd908eda73e670af888da41af171505")),
	}
	bytes, err := json.Marshal(vr)
	if err != nil {
		t.Fatal(err)
	}
	result := &ValidatorRegistration{}
	err = json.Unmarshal(bytes, result)
	if err != nil {
		t.Fatal(err)
	}
	// expect result == vr
	if result.Message.FeeRecipient != vr.Message.FeeRecipient {
		t.Fatalf("expected %v, got %v", vr.Message.FeeRecipient, result.Message.FeeRecipient)
	}
	if result.Message.GasLimit != vr.Message.GasLimit {
		t.Fatalf("expected %v, got %v", vr.Message.GasLimit, result.Message.GasLimit)
	}
	if result.Message.Timestamp != vr.Message.Timestamp {
		t.Fatalf("expected %v, got %v", vr.Message.Timestamp, result.Message.Timestamp)
	}
	if result.Message.PubKey != vr.Message.PubKey {
		t.Fatalf("expected %v, got %v", vr.Message.PubKey, result.Message.PubKey)
	}
	if result.Signature != vr.Signature {
		t.Fatalf("expected %v, got %v", vr.Signature, result.Signature)
	}
}
