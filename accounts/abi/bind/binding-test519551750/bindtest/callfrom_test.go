
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/common"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestCallFrom(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth := bind.NewKeyedTransactor(key)

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			// Deploy a sender tester contract and execute a structured call on it
			_, _, callfrom, err := DeployCallFrom(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy sender contract: %v", err)
			}
			sim.Commit()

			if res, err := callfrom.CallFrom(nil); err != nil {
				t.Errorf("Failed to call constant function: %v", err)
			} else if res != (common.Address{}) {
				t.Errorf("Invalid address returned, want: %x, got: %x", (common.Address{}), res)
			}

			for _, addr := range []common.Address{common.Address{}, common.Address{1}, common.Address{2}} {
				if res, err := callfrom.CallFrom(&bind.CallOpts{From: addr}); err != nil {
					t.Fatalf("Failed to call constant function: %v", err)
				} else if res != addr {
					t.Fatalf("Invalid address returned, want: %x, got: %x", addr, res)
				}
			}
		
			}
		