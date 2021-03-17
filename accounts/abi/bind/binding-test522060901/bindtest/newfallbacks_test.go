
			package bindtest

			import (
				"testing"
				
			"bytes"
			"math/big"
	
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
	   
			)

			func TestNewFallbacks(t *testing.T) {
				
			key, _ := crypto.GenerateKey()
			addr := crypto.PubkeyToAddress(key.PublicKey)
	
			sim := backends.NewSimulatedBackend(core.GenesisAlloc{addr: {Balance: big.NewInt(1000000000)}}, 1000000)
			defer sim.Close()
	
			opts, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
			_, _, c, err := DeployNewFallbacks(opts, sim)
			if err != nil {
				t.Fatalf("Failed to deploy contract: %v", err)
			}
			sim.Commit()
	
			// Test receive function
			opts.Value = big.NewInt(100)
			c.Receive(opts)
			sim.Commit()
	
			var gotEvent bool
			iter, _ := c.FilterReceived(nil)
			defer iter.Close()
			for iter.Next() {
				if iter.Event.Addr != addr {
					t.Fatal("Msg.sender mismatch")
				}
				if iter.Event.Value.Uint64() != 100 {
					t.Fatal("Msg.value mismatch")
				}
				gotEvent = true
				break
			}
			if !gotEvent {
				t.Fatal("Expect to receive event emitted by receive")
			}
	
			// Test fallback function
			gotEvent = false
			opts.Value = nil
			calldata := []byte{0x01, 0x02, 0x03}
			c.Fallback(opts, calldata)
			sim.Commit()
	
			iter2, _ := c.FilterFallback(nil)
			defer iter2.Close()
			for iter2.Next() {
				if !bytes.Equal(iter2.Event.Data, calldata) {
					t.Fatal("calldata mismatch")
				}
				gotEvent = true
				break
			}
			if !gotEvent {
				t.Fatal("Expect to receive event emitted by fallback")
			}
	   
			}
		