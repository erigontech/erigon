package services_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"testing/fstest"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cmd/starknet/services"

	"github.com/ledgerwatch/erigon/crypto"
)

func TestCreate(t *testing.T) {
	privateKey := "26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48"

	var cases = []struct {
		name       string
		privateKey string
		config     *services.Config
		want       string
		error      error
	}{
		{name: "invalid private key", privateKey: "abc", config: &services.Config{
			ContractFileName: "not_exist.json",
		}, error: services.ErrInvalidPrivateKey},
		{name: "contract file not found", privateKey: generatePrivateKey(t), config: &services.Config{
			ContractFileName: "not_exist.json",
		}, error: services.ErrReadContract},
		{name: "success", privateKey: privateKey, config: &services.Config{
			ContractFileName: "contract_test.json",
			Salt:             []byte("contract_address_salt"),
			Gas:              1,
			Nonce:            0,
		}, want: "0xb88503f88283127ed801830186a084342770c0018001963762323236313632363932323361323035623564376495636f6e74726163745f616464726573735f73616c74c080a08b88467d0a9a6cba87ec6c2ad9e7399d12a1b6f7f5b951bdd2c5c2ea08b76134a0472e1b37ca5f87c9c38690718c6b2b9db1a3d5398dc664fc4e158ab60d02d64b"},
	}

	fs := fstest.MapFS{
		"contract_test.json": {Data: []byte("{\"abi\": []}")},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rawTxGenerator := services.NewRawTxGenerator(tt.privateKey)

			ctx := context.Background()
			buf := bytes.NewBuffer(nil)
			db := memdb.NewTestDB(t)

			err := rawTxGenerator.CreateFromFS(ctx, fs, db, tt.config, buf)

			if tt.error == nil {
				assertNoError(t, err)

				got := buf.String()

				if got != tt.want {
					t.Errorf("got %q not equals want %q", got, tt.want)
				}
			} else {
				assertError(t, err, tt.error)
			}
		})
	}
}

func generatePrivateKey(t testing.TB) string {
	t.Helper()

	privateKey, err := crypto.GenerateKey()
	if err != nil {
		t.Error(err)
	}

	return hex.EncodeToString(crypto.FromECDSA(privateKey))
}

func assertNoError(t testing.TB, got error) {
	t.Helper()

	if got != nil {
		t.Fatal("got an error but didn't want one")
	}
}

func assertError(t testing.TB, got error, want error) {
	t.Helper()

	if got == nil {
		t.Fatal("didn't get an error but wanted one")
	}

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
