
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestUseLibrary(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth := bind.NewKeyedTransactor(key)

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			//deploy the test contract
			_, _, testContract, err := DeployUseLibrary(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy test contract: %v", err)
			}

			// Finish deploy.
			sim.Commit()

			// Check that the library contract has been deployed
			// by calling the contract's add function.
			res, err := testContract.Add(&bind.CallOpts{
				From: auth.From,
				Pending: false,
			}, big.NewInt(1), big.NewInt(2))
			if err != nil {
				t.Fatalf("Failed to call linked contract: %v", err)
			}
			if res.Cmp(big.NewInt(3)) != 0 {
				t.Fatalf("Add did not return the correct result: %d != %d", res, 3)
			}
		
			}
		