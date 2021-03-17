
			package bindtest

			import (
				"testing"
				
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestDeeplyNestedArray(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			//deploy the test contract
			_, _, testContract, err := DeployDeeplyNestedArray(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy test contract: %v", err)
			}

			// Finish deploy.
			sim.Commit()

			//Create coordinate-filled array, for testing purposes.
			testArr := [5][4][3]uint64{}
			for i := 0; i < 5; i++ {
				testArr[i] = [4][3]uint64{}
				for j := 0; j < 4; j++ {
					testArr[i][j] = [3]uint64{}
					for k := 0; k < 3; k++ {
						//pack the coordinates, each array value will be unique, and can be validated easily.
						testArr[i][j][k] = uint64(i) << 16 | uint64(j) << 8 | uint64(k)
					}
				}
			}

			if _, err := testContract.StoreDeepUintArray(&bind.TransactOpts{
				From: auth.From,
				Signer: auth.Signer,
			}, testArr); err != nil {
				t.Fatalf("Failed to store nested array in test contract: %v", err)
			}

			sim.Commit()

			retrievedArr, err := testContract.RetrieveDeepArray(&bind.CallOpts{
				From: auth.From,
				Pending: false,
			})
			if err != nil {
				t.Fatalf("Failed to retrieve nested array from test contract: %v", err)
			}

			//quick check to see if contents were copied
			// (See accounts/abi/unpack_test.go for more extensive testing)
			if retrievedArr[4][3][2] != testArr[4][3][2] {
				t.Fatalf("Retrieved value does not match expected value! got: %d, expected: %d. %v", retrievedArr[4][3][2], testArr[4][3][2], err)
			}
		
			}
		