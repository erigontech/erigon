package services_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/ledgerwatch/erigon/cmd/starknet/services"
	"testing"
	"testing/fstest"

	"github.com/ledgerwatch/erigon/crypto"
)

func TestCreate(t *testing.T) {
	privateKey := "26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48"

	var cases = []struct {
		name       string
		privateKey string
		fileName   string
		salt       string
		gas        uint64
		want       string
		error      error
	}{
		{name: "invalid private key", privateKey: "abc", fileName: "not_exist.json", error: services.ErrInvalidPrivateKey},
		{name: "contract file not found", privateKey: generatePrivateKey(t), fileName: "not_exist.json", error: services.ErrReadContract},
		{name: "success", privateKey: privateKey, fileName: "contract_test.json", salt: "contract_address_salt", gas: 1, want: "03f88283127ed880830186a084342770c0018001963762323236313632363932323361323035623564376495636f6e74726163745f616464726573735f73616c74c001a0f4c886792b3b0c92789b18e80cc8587715b40c51b4d85c8c2106f536506f22aba02855010af44a167d4a0c1b3ab38e7757bc45a4c31693a785f35172e510fd77ad"},
	}

	fs := fstest.MapFS{
		"contract_test.json": {Data: []byte("{\"abi\": []}")},
	}
	store := services.NewStubStore(map[string]int{
		"123": 1,
	})

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rawTxGenerator := services.NewRawTxGenerator(tt.privateKey)

			ctx := context.Background()
			buf := bytes.NewBuffer(nil)
			err := rawTxGenerator.CreateFromFS(ctx, fs, store, tt.fileName, []byte(tt.salt), tt.gas, buf)

			if tt.error == nil {
				assertNoError(t, err)

				got := hex.EncodeToString(buf.Bytes())

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
