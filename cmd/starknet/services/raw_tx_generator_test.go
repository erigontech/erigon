package services

import (
	"bytes"
	"encoding/hex"
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
		{name: "invalid private key", privateKey: "abc", fileName: "not_exist.json", error: ErrInvalidPrivateKey},
		{name: "contract file not found", privateKey: generatePrivateKey(t), fileName: "not_exist.json", error: ErrReadContract},
		{name: "success", privateKey: privateKey, fileName: "contract_test.json", salt: "contract_address_salt", gas: 1, want: "03f88183127ed802830186a0830186a0018001963762323236313632363932323361323035623564376495636f6e74726163745f616464726573735f73616c74c001a0d76aefa12efeb7602858aba7889b5aa89eaca8e530979a9638e8b15b6c818a9aa07737ddf3e01ca1dfac1562eac7b1117a26ad3322621ddb6c4bee71352821698c"},
	}

	fs := fstest.MapFS{
		"contract_test.json": {Data: []byte("{\"abi\": []}")},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rawTxGenerator := RawTxGenerator{
				privateKey: tt.privateKey,
			}

			buf := bytes.NewBuffer(nil)
			err := rawTxGenerator.CreateFromFS(fs, tt.fileName, []byte(tt.salt), tt.gas, buf)

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
