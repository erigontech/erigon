
			package bindtest

			import (
				"testing"
				
		"math/big"

		"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
		"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
		"github.com/ledgerwatch/turbo-geth/crypto"
		"github.com/ledgerwatch/turbo-geth/core"
        
			)

			func TestMultiContracts(t *testing.T) {
				
		key, _ := crypto.GenerateKey()
		addr := crypto.PubkeyToAddress(key.PublicKey)

		// Deploy registrar contract
		sim := backends.NewSimulatedBackend(core.GenesisAlloc{addr: {Balance: big.NewInt(1000000000)}}, 10000000)
		defer sim.Close()

		transactOpts := bind.NewKeyedTransactor(key)
		_, _, c1, err := DeployContractOne(transactOpts, sim)
		if err != nil {
			t.Fatal("Failed to deploy contract")
		}
		sim.Commit()
		err = c1.Foo(nil, ExternalLibSharedStruct{
			F1: big.NewInt(100),
			F2: [32]byte{0x01, 0x02, 0x03},
		})
		if err != nil {
			t.Fatal("Failed to invoke function")
		}
		_, _, c2, err := DeployContractTwo(transactOpts, sim)
		if err != nil {
			t.Fatal("Failed to deploy contract")
		}
		sim.Commit()
		err = c2.Bar(nil, ExternalLibSharedStruct{
			F1: big.NewInt(100),
			F2: [32]byte{0x01, 0x02, 0x03},
		})
		if err != nil {
			t.Fatal("Failed to invoke function")
		}
        
			}
		