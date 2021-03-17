
			package bindtest

			import (
				"testing"
				
		"math/big"
		"time"

		"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
		"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
		"github.com/ledgerwatch/turbo-geth/core"
		"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestOverload(t *testing.T) {
				
		// Initialize test accounts
		key, _ := crypto.GenerateKey()
		auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
		sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
		defer sim.Close()

		// deploy the test contract
		_, _, contract, err := DeployOverload(auth, sim)
		if err != nil {
			t.Fatalf("Failed to deploy contract: %v", err)
		}
		// Finish deploy.
		sim.Commit()

		resCh, stopCh := make(chan uint64), make(chan struct{})

		go func() {
			barSink := make(chan *OverloadBar)
			sub, _ := contract.WatchBar(nil, barSink)
			defer sub.Unsubscribe()

			bar0Sink := make(chan *OverloadBar0)
			sub0, _ := contract.WatchBar0(nil, bar0Sink)
			defer sub0.Unsubscribe()

			for {
				select {
				case ev := <-barSink:
					resCh <- ev.I.Uint64()
				case ev := <-bar0Sink:
					resCh <- ev.I.Uint64() + ev.J.Uint64()
				case <-stopCh:
					return
				}
			}
		}()
		contract.Foo(auth, big.NewInt(1), big.NewInt(2))
		sim.Commit()
		select {
		case n := <-resCh:
			if n != 3 {
				t.Fatalf("Invalid bar0 event")
			}
		case <-time.NewTimer(3 * time.Second).C:
			t.Fatalf("Wait bar0 event timeout")
		}

		contract.Foo0(auth, big.NewInt(1))
		sim.Commit()
		select {
		case n := <-resCh:
			if n != 1 {
				t.Fatalf("Invalid bar event")
			}
		case <-time.NewTimer(3 * time.Second).C:
			t.Fatalf("Wait bar event timeout")
		}
		close(stopCh)
		
			}
		