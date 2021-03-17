
			package bindtest

			import (
				"testing"
				
			"math/big"
			"reflect"

			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
			"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
			"github.com/ledgerwatch/turbo-geth/core"
			"github.com/ledgerwatch/turbo-geth/crypto"
		
			)

			func TestTuple(t *testing.T) {
				
			key, _ := crypto.GenerateKey()
			auth, _ := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))

			sim := backends.NewSimulatedBackend(core.GenesisAlloc{auth.From: {Balance: big.NewInt(10000000000)}}, 10000000)
			defer sim.Close()

			_, _, contract, err := DeployTuple(auth, sim)
			if err != nil {
				t.Fatalf("deploy contract failed %v", err)
			}
			sim.Commit()

			check := func(a, b interface{}, errMsg string) {
				if !reflect.DeepEqual(a, b) {
					t.Fatal(errMsg)
				}
			}

			a := TupleS{
				A: big.NewInt(1),
				B: []*big.Int{big.NewInt(2), big.NewInt(3)},
				C: []TupleT{
					{
						X: big.NewInt(4),
						Y: big.NewInt(5),
					},
					{
						X: big.NewInt(6),
						Y: big.NewInt(7),
					},
				},
			}

			b := [][2]TupleT{
				{
					{
						X: big.NewInt(8),
						Y: big.NewInt(9),
					},
					{
						X: big.NewInt(10),
						Y: big.NewInt(11),
					},
				},
			}

			c := [2][]TupleT{
				{
					{
						X: big.NewInt(12),
						Y: big.NewInt(13),
					},
					{
						X: big.NewInt(14),
						Y: big.NewInt(15),
					},
				},
				{
					{
						X: big.NewInt(16),
						Y: big.NewInt(17),
					},
				},
			}

			d := []TupleS{a}

			e := []*big.Int{big.NewInt(18), big.NewInt(19)}
			ret1, ret2, ret3, ret4, ret5, err := contract.Func1(nil, a, b, c, d, e)
			if err != nil {
				t.Fatalf("invoke contract failed, err %v", err)
			}
			check(ret1, a, "ret1 mismatch")
			check(ret2, b, "ret2 mismatch")
			check(ret3, c, "ret3 mismatch")
			check(ret4, d, "ret4 mismatch")
			check(ret5, e, "ret5 mismatch")

			_, err = contract.Func2(auth, a, b, c, d, e)
			if err != nil {
				t.Fatalf("invoke contract failed, err %v", err)
			}
			sim.Commit()

			iter, err := contract.FilterTupleEvent(nil)
			if err != nil {
				t.Fatalf("failed to create event filter, err %v", err)
			}
			defer iter.Close()

			iter.Next()
			check(iter.Event.A, a, "field1 mismatch")
			check(iter.Event.B, b, "field2 mismatch")
			check(iter.Event.C, c, "field3 mismatch")
			check(iter.Event.D, d, "field4 mismatch")
			check(iter.Event.E, e, "field5 mismatch")

			err = contract.Func3(nil, nil)
			if err != nil {
				t.Fatalf("failed to call function which has no return, err %v", err)
			}
		
			}
		