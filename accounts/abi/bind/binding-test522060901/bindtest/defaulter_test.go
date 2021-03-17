
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestDefaulter(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			// Deploy a default method invoker contract and execute its default method
			_, _, defaulter, err := DeployDefaulter(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy defaulter contract: %v", err)
			}
			if _, err := (&DefaulterRaw{defaulter}).Transfer(auth); err != nil {
				t.Fatalf("Failed to invoke default method: %v", err)
			}
			sim.Commit()

			if caller, err := defaulter.Caller(nil); err != nil {
				t.Fatalf("Failed to call address retriever: %v", err)
			} else if (caller != auth.From) {
				t.Fatalf("Address mismatch: have %v, want %v", caller, auth.From)
			}
		
			}
		