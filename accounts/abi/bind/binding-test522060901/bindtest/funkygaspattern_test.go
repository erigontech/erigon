
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestFunkyGasPattern(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			// Deploy a funky gas pattern contract
			_, _, limiter, err := DeployFunkyGasPattern(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy funky contract: %v", err)
			}
			sim.Commit()

			// Set the field with automatic estimation and check that it succeeds
			if _, err := limiter.SetField(auth, "automatic"); err != nil {
				t.Fatalf("Failed to call automatically gased transaction: %v", err)
			}
			sim.Commit()

			if field, _ := limiter.Field(nil); field != "automatic" {
				t.Fatalf("Field mismatch: have %v, want %v", field, "automatic")
			}
		
			}
		