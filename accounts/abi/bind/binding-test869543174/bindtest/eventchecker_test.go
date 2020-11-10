
			package bindtest

			import (
				"testing"
				
			"fmt"
			"math/big"
			"reflect"

			"github.com/ledgerwatch/turbo-geth/common"
		
			)

			func TestEventChecker(t *testing.T) {
				if e, err := NewEventChecker(common.Address{}, nil); e == nil || err != nil {
			 t.Fatalf("binding (%v) nil or error (%v) not nil", e, nil)
		 } else if false { // Don't run, just compile and test types
			 var (
				 err  error
			   res  bool
				 str  string
				 dat  []byte
				 hash common.Hash
			 )
			 _, err = e.FilterEmpty(nil)
			 _, err = e.FilterIndexed(nil, []common.Address{}, []*big.Int{})

			 mit, err := e.FilterMixed(nil, []common.Address{})

			 res = mit.Next()  // Make sure the iterator has a Next method
			 err = mit.Error() // Make sure the iterator has an Error method
			 err = mit.Close() // Make sure the iterator has a Close method

			 fmt.Println(mit.Event.Raw.BlockHash) // Make sure the raw log is contained within the results
			 fmt.Println(mit.Event.Num)           // Make sure the unpacked non-indexed fields are present
			 fmt.Println(mit.Event.Addr)          // Make sure the reconstructed indexed fields are present

			 dit, err := e.FilterDynamic(nil, []string{}, [][]byte{})

			 str  = dit.Event.Str    // Make sure non-indexed strings retain their type
			 dat  = dit.Event.Dat    // Make sure non-indexed bytes retain their type
			 hash = dit.Event.IdxStr // Make sure indexed strings turn into hashes
			 hash = dit.Event.IdxDat // Make sure indexed bytes turn into hashes

			 sink := make(chan *EventCheckerMixed)
			 sub, err := e.WatchMixed(nil, sink, []common.Address{})
			 defer sub.Unsubscribe()

			 event := <-sink
			 fmt.Println(event.Raw.BlockHash) // Make sure the raw log is contained within the results
			 fmt.Println(event.Num)           // Make sure the unpacked non-indexed fields are present
			 fmt.Println(event.Addr)          // Make sure the reconstructed indexed fields are present

			 fmt.Println(res, str, dat, hash, err)

			 oit, err := e.FilterUnnamed(nil, []*big.Int{}, []*big.Int{})

			 arg0  := oit.Event.Arg0    // Make sure unnamed arguments are handled correctly
			 arg1  := oit.Event.Arg1    // Make sure unnamed arguments are handled correctly
			 fmt.Println(arg0, arg1)
		 }
		 // Run a tiny reflection test to ensure disallowed methods don't appear
		 if _, ok := reflect.TypeOf(&EventChecker{}).MethodByName("FilterAnonymous"); ok {
		 	t.Errorf("binding has disallowed method (FilterAnonymous)")
		 }
			}
		