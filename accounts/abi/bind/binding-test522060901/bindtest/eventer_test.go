
			package bindtest

			import (
				"testing"
				
				"math/big"
				"time"

				"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
				"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
				"github.com/ledgerwatch/turbo-geth/common"
				"github.com/ledgerwatch/turbo-geth/core"
				"github.com/ledgerwatch/turbo-geth/crypto"
			
			)

			func TestEventer(t *testing.T) {
				
			// Generate a new random account and a funded simulator
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

				sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
				defer sim.Close()

				// Deploy an eventer contract
				_, _, eventer, err := DeployEventer(auth, sim)
				if err != nil {
					t.Fatalf("Failed to deploy eventer contract: %v", err)
				}
				sim.Commit()

				// Inject a few events into the contract, gradually more in each block
				for i := 1; i <= 3; i++ {
					for j := 1; j <= i; j++ {
						if _, err := eventer.RaiseSimpleEvent(auth, common.Address{byte(j)}, [32]byte{byte(j)}, true, big.NewInt(int64(10*i+j))); err != nil {
							t.Fatalf("block %d, event %d: raise failed: %v", i, j, err)
						}
					}
					sim.Commit()
				}
				// Test filtering for certain events and ensure they can be found
				sit, err := eventer.FilterSimpleEvent(nil, []common.Address{common.Address{1}, common.Address{3}}, [][32]byte{{byte(1)}, {byte(2)}, {byte(3)}}, []bool{true})
				if err != nil {
					t.Fatalf("failed to filter for simple events: %v", err)
				}
				defer sit.Close()

				sit.Next()
				if sit.Event.Value.Uint64() != 11 || !sit.Event.Flag {
					t.Errorf("simple log content mismatch: have %v, want {11, true}", sit.Event)
				}
				sit.Next()
				if sit.Event.Value.Uint64() != 21 || !sit.Event.Flag {
					t.Errorf("simple log content mismatch: have %v, want {21, true}", sit.Event)
				}
				sit.Next()
				if sit.Event.Value.Uint64() != 31 || !sit.Event.Flag {
					t.Errorf("simple log content mismatch: have %v, want {31, true}", sit.Event)
				}
				sit.Next()
				if sit.Event.Value.Uint64() != 33 || !sit.Event.Flag {
					t.Errorf("simple log content mismatch: have %v, want {33, true}", sit.Event)
				}

				if sit.Next() {
					t.Errorf("unexpected simple event found: %+v", sit.Event)
				}
				if err = sit.Error(); err != nil {
					t.Fatalf("simple event iteration failed: %v", err)
				}
				// Test raising and filtering for an event with no data component
				if _, err := eventer.RaiseNodataEvent(auth, big.NewInt(314), 141, 271); err != nil {
					t.Fatalf("failed to raise nodata event: %v", err)
				}
				sim.Commit()

				nit, err := eventer.FilterNodataEvent(nil, []*big.Int{big.NewInt(314)}, []int16{140, 141, 142}, []uint32{271})
				if err != nil {
					t.Fatalf("failed to filter for nodata events: %v", err)
				}
				defer nit.Close()

				if !nit.Next() {
					t.Fatalf("nodata log not found: %v", nit.Error())
				}
				if nit.Event.Number.Uint64() != 314 {
					t.Errorf("nodata log content mismatch: have %v, want 314", nit.Event.Number)
				}
				if nit.Next() {
					t.Errorf("unexpected nodata event found: %+v", nit.Event)
				}
				if err = nit.Error(); err != nil {
					t.Fatalf("nodata event iteration failed: %v", err)
				}
				// Test raising and filtering for events with dynamic indexed components
				if _, err := eventer.RaiseDynamicEvent(auth, "Hello", []byte("World")); err != nil {
					t.Fatalf("failed to raise dynamic event: %v", err)
				}
				sim.Commit()

				dit, err := eventer.FilterDynamicEvent(nil, []string{"Hi", "Hello", "Bye"}, [][]byte{[]byte("World")})
				if err != nil {
					t.Fatalf("failed to filter for dynamic events: %v", err)
				}
				defer dit.Close()

				if !dit.Next() {
					t.Fatalf("dynamic log not found: %v", dit.Error())
				}
				if dit.Event.NonIndexedString != "Hello" || string(dit.Event.NonIndexedBytes) != "World" ||	dit.Event.IndexedString != common.HexToHash("0x06b3dfaec148fb1bb2b066f10ec285e7c9bf402ab32aa78a5d38e34566810cd2") || dit.Event.IndexedBytes != common.HexToHash("0xf2208c967df089f60420785795c0a9ba8896b0f6f1867fa7f1f12ad6f79c1a18") {
					t.Errorf("dynamic log content mismatch: have %v, want {'0x06b3dfaec148fb1bb2b066f10ec285e7c9bf402ab32aa78a5d38e34566810cd2, '0xf2208c967df089f60420785795c0a9ba8896b0f6f1867fa7f1f12ad6f79c1a18', 'Hello', 'World'}", dit.Event)
				}
				if dit.Next() {
					t.Errorf("unexpected dynamic event found: %+v", dit.Event)
				}
				if err = dit.Error(); err != nil {
					t.Fatalf("dynamic event iteration failed: %v", err)
				}
				// Test raising and filtering for events with fixed bytes components
				var fblob [24]byte
				copy(fblob[:], []byte("Fixed Bytes"))

				if _, err := eventer.RaiseFixedBytesEvent(auth, fblob); err != nil {
					t.Fatalf("failed to raise fixed bytes event: %v", err)
				}
				sim.Commit()

				fit, err := eventer.FilterFixedBytesEvent(nil, [][24]byte{fblob})
				if err != nil {
					t.Fatalf("failed to filter for fixed bytes events: %v", err)
				}
				defer fit.Close()

				if !fit.Next() {
					t.Fatalf("fixed bytes log not found: %v", fit.Error())
				}
				if fit.Event.NonIndexedBytes != fblob || fit.Event.IndexedBytes != fblob {
					t.Errorf("fixed bytes log content mismatch: have %v, want {'%x', '%x'}", fit.Event, fblob, fblob)
				}
				if fit.Next() {
					t.Errorf("unexpected fixed bytes event found: %+v", fit.Event)
				}
				if err = fit.Error(); err != nil {
					t.Fatalf("fixed bytes event iteration failed: %v", err)
				}
				// Test subscribing to an event and raising it afterwards
				ch := make(chan *EventerSimpleEvent, 16)
				sub, err := eventer.WatchSimpleEvent(nil, ch, nil, nil, nil)
				if err != nil {
					t.Fatalf("failed to subscribe to simple events: %v", err)
				}
				if _, err := eventer.RaiseSimpleEvent(auth, common.Address{255}, [32]byte{255}, true, big.NewInt(255)); err != nil {
					t.Fatalf("failed to raise subscribed simple event: %v", err)
				}
				sim.Commit()

				select {
				case event := <-ch:
					if event.Value.Uint64() != 255 {
						t.Errorf("simple log content mismatch: have %v, want 255", event)
					}
				case <-time.After(250 * time.Millisecond):
					t.Fatalf("subscribed simple event didn't arrive")
				}
				// Unsubscribe from the event and make sure we're not delivered more
				sub.Unsubscribe()

				if _, err := eventer.RaiseSimpleEvent(auth, common.Address{254}, [32]byte{254}, true, big.NewInt(254)); err != nil {
					t.Fatalf("failed to raise subscribed simple event: %v", err)
				}
				sim.Commit()

				select {
				case event := <-ch:
					t.Fatalf("unsubscribed simple event arrived: %v", event)
				case <-time.After(250 * time.Millisecond):
				}
			
			}
		