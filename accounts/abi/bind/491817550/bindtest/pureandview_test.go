
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestPureAndView(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth := bind.NewKeyedTransactor(key)

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			// Deploy a tester contract and execute a structured call on it
			_, _, pav, err := DeployPureAndView(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy PureAndView contract: %v", err)
			}
			sim.Commit()

			// This test the existence of the free retreiver call for view and pure functions
			if num, err := pav.PureFunc(nil); err != nil {
				t.Fatalf("Failed to call anonymous field retriever: %v", err)
			} else if num.Cmp(big.NewInt(42)) != 0 {
				t.Fatalf("Retrieved value mismatch: have %v, want %v", num, 42)
			}
			if num, err := pav.ViewFunc(nil); err != nil {
				t.Fatalf("Failed to call anonymous field retriever: %v", err)
			} else if num.Cmp(big.NewInt(2)) != 0 {
				t.Fatalf("Retrieved value mismatch: have %v, want %v", num, 1)
			}
		
			}
		