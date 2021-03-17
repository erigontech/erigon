
			package bindtest

			import (
				"testing"
				
		"math/big"

		"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
		"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
		"github.com/ledgerwatch/turbo-geth/crypto"
		"github.com/ledgerwatch/turbo-geth/core"
		
			)

			func TestIdentifierCollision(t *testing.T) {
				
		// Initialize test accounts
		key, _ := crypto.GenerateKey()
		addr := crypto.PubkeyToAddress(key.PublicKey)

		// Deploy registrar contract
		sim := backends.NewSimulatedBackend(core.GenesisAlloc{addr: {Balance: big.NewInt(1000000000)}}, 10000000)
		defer sim.Close()

		transactOpts, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
		_, _, _, err := DeployIdentifierCollision(transactOpts, sim)
		if err != nil {
			t.Fatalf("failed to deploy contract: %v", err)
		}
		
			}
		