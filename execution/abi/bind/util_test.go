// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bind_test

import (
	"context"
	"errors"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/abi/bind/backends"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

var waitDeployedTests = map[string]struct {
	code        string
	gas         uint64
	wantAddress common.Address
	wantErr     error
}{
	"successful deploy": {
		code:        `6060604052600a8060106000396000f360606040526008565b00`,
		gas:         3000000,
		wantAddress: common.HexToAddress("0x3a220f351252089d385b29beca14e27f204c296a"),
	},
	"empty code": {
		code:        ``,
		gas:         300000,
		wantErr:     bind.ErrNoCodeAfterDeploy,
		wantAddress: common.HexToAddress("0x3a220f351252089d385b29beca14e27f204c296a"),
	},
}

func TestWaitDeployed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	for name, test := range waitDeployedTests {
		name := name
		test := test

		t.Run(name, func(t *testing.T) {
			backend := backends.NewSimulatedBackend(t,
				types.GenesisAlloc{
					crypto.PubkeyToAddress(testKey.PublicKey): {Balance: big.NewInt(10000000000)},
				},
				10000000,
			)
			if backend.HistoryV3() {
				t.Skip("HistoryV3 doesn't store receipts")
			}

			// Create the transaction.
			// Create the transaction.
			var txn types.Transaction = types.NewContractCreation(0, u256.Num0, test.gas, u256.Num1, common.FromHex(test.code))
			signer := types.MakeSigner(chain.TestChainConfig, 1, 0)
			txn, _ = types.SignTx(txn, *signer, testKey)

			// Wait for it to get mined in the background.
			var (
				err     error
				address common.Address
				mined   = make(chan struct{})
				ctx     = context.Background()
			)

			// Send and mine the transaction.
			if err = backend.SendTransaction(ctx, txn); err != nil {
				t.Fatalf("test %q: failed to set tx: %v", name, err)
			}
			backend.Commit()

			go func() {
				address, err = bind.WaitDeployed(ctx, backend, txn)
				close(mined)
			}()

			select {
			case <-mined:
				if !errors.Is(err, test.wantErr) {
					t.Errorf("test %q: error mismatch: want %q, got %q", name, test.wantErr, err)
				}
				if address != test.wantAddress {
					t.Errorf("test %q: unexpected contract address %s", name, address.Hex())
				}
			case <-time.After(2 * time.Second):
				t.Errorf("test %q: timeout", name)
			}
		})
	}
}

func TestWaitDeployedCornerCases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	backend := backends.NewSimulatedBackend(t,
		types.GenesisAlloc{
			crypto.PubkeyToAddress(testKey.PublicKey): {Balance: big.NewInt(10000000000)},
		},
		10000000,
	)

	// Create a transaction to an account.
	code := "6060604052600a8060106000396000f360606040526008565b00"
	signer := types.MakeSigner(chain.TestChainConfig, 1, 0)
	var txn types.Transaction = types.NewTransaction(0, common.HexToAddress("0x01"), u256.Num0, 3000000, u256.Num1, common.FromHex(code))
	txn, _ = types.SignTx(txn, *signer, testKey)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := backend.SendTransaction(ctx, txn); err != nil {
		t.Errorf("error when sending tx: %v", err)
	}
	backend.Commit()
	notContentCreation := errors.New("tx is not contract creation")
	if _, err := bind.WaitDeployed(ctx, backend, txn); err.Error() != notContentCreation.Error() {
		t.Errorf("error mismatch: want %q, got %q, ", notContentCreation, err)
	}

	// Create a transaction that is not mined.
	txn = types.NewContractCreation(1, u256.Num0, 3000000, u256.Num1, common.FromHex(code))
	txn, _ = types.SignTx(txn, *signer, testKey)

	if err := backend.SendTransaction(ctx, txn); err != nil {
		t.Errorf("error when sending tx: %v", err)
	}

	done := make(chan bool)
	go func() {
		defer close(done)
		contextCanceled := errors.New("context canceled")
		if _, err := bind.WaitDeployed(ctx, backend, txn); err.Error() != contextCanceled.Error() {
			t.Errorf("error missmatch: want %q, got %q, ", contextCanceled, err)
		}
		done <- true
	}()

	cancel()
	<-done
}
