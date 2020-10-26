
			package bindtest

			import (
				"testing"
				"github.com/ledgerwatch/turbo-geth/common"
			)

			func TestCrowdsale(t *testing.T) {
				
			if b, err := NewCrowdsale(common.Address{}, nil); b == nil || err != nil {
				t.Fatalf("binding (%v) nil or error (%v) not nil", b, nil)
			}
		
			}
		