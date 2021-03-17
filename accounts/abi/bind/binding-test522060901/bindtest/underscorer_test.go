
			package bindtest

			import (
				"testing"
				
			"fmt"
			"math/big"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestUnderscorer(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			// Deploy a underscorer tester contract and execute a structured call on it
			_, _, underscorer, err := DeployUnderscorer(auth, sim)
			if err != nil {
				t.Fatalf("Failed to deploy underscorer contract: %v", err)
			}
			sim.Commit()

			// Verify that underscored return values correctly parse into structs
			if res, err := underscorer.UnderscoredOutput(nil); err != nil {
				t.Errorf("Failed to call constant function: %v", err)
			} else if res.Int.Cmp(big.NewInt(314)) != 0 || res.String != "pi" {
				t.Errorf("Invalid result, want: {314, \"pi\"}, got: %+v", res)
			}
			// Verify that underscored and non-underscored name collisions force tuple outputs
			var a, b *big.Int

			a, b, _ = underscorer.LowerLowerCollision(nil)
			a, b, _ = underscorer.LowerUpperCollision(nil)
			a, b, _ = underscorer.UpperLowerCollision(nil)
			a, b, _ = underscorer.UpperUpperCollision(nil)
			a, b, _ = underscorer.PurelyUnderscoredOutput(nil)
			a, b, _ = underscorer.AllPurelyUnderscoredOutput(nil)
			a, _ = underscorer.UnderScoredFunc(nil)

			fmt.Println(a, b, err)
		
			}
		