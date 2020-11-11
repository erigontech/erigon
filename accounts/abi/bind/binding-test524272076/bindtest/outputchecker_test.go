
			package bindtest

			import (
				"testing"
				
			"fmt"

			"github.com/ledgerwatch/turbo-geth/common"
		
			)

			func TestOutputChecker(t *testing.T) {
				if b, err := NewOutputChecker(common.Address{}, nil); b == nil || err != nil {
			 t.Fatalf("binding (%v) nil or error (%v) not nil", b, nil)
		 } else if false { // Don't run, just compile and test types
			 var str1, str2 string
			 var err error

			 err              = b.NoOutput(nil)
			 str1, err        = b.NamedOutput(nil)
			 str1, err        = b.AnonOutput(nil)
			 res, _          := b.NamedOutputs(nil)
			 str1, str2, err  = b.CollidingOutputs(nil)
			 str1, str2, err  = b.AnonOutputs(nil)
			 str1, str2, err  = b.MixedOutputs(nil)

			 fmt.Println(str1, str2, res.Str1, res.Str2, err)
		 }
			}
		