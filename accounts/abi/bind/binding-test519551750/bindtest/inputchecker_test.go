
			package bindtest

			import (
				"testing"
				
			"fmt"

			"github.com/ledgerwatch/turbo-geth/common"
		
			)

			func TestInputChecker(t *testing.T) {
				if b, err := NewInputChecker(common.Address{}, nil); b == nil || err != nil {
			 t.Fatalf("binding (%v) nil or error (%v) not nil", b, nil)
		 } else if false { // Don't run, just compile and test types
			 var err error

			 err = b.NoInput(nil)
			 err = b.NamedInput(nil, "")
			 err = b.AnonInput(nil, "")
			 err = b.NamedInputs(nil, "", "")
			 err = b.AnonInputs(nil, "", "")
			 err = b.MixedInputs(nil, "", "")

			 fmt.Println(err)
		 }
			}
		