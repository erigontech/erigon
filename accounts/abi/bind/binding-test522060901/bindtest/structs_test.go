
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestStructs(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
		
			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()
		
			// Deploy a structs method invoker contract and execute its default method
			_, _, structs, err := DeployStructs(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy defaulter contract: %v", err)
			}
			sim.Commit()
			opts := bind.CallOpts{}
			if _, err := structs.F(&opts); err != nil {
				t.Fatalf("Failed to invoke F method: %v", err)
			}
			if _, err := structs.G(&opts); err != nil {
				t.Fatalf("Failed to invoke G method: %v", err)
			}
		
			}
		