
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestGetter(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			// Deploy a tuple tester contract and execute a structured call on it
			_, _, getter, err := DeployGetter(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy getter contract: %v", err)
			}
			sim.Commit()

			if str, num, _, err := getter.Getter(nil); err != nil {
				t.Fatalf("Failed to call anonymous field retriever: %v", err)
			} else if str != "Hi" || num.Cmp(big.NewInt(1)) != 0 {
				t.Fatalf("Retrieved value mismatch: have %v/%v, want %v/%v", str, num, "Hi", 1)
			}
		
			}
		