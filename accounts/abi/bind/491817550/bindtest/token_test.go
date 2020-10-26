
			package bindtest

			import (
				"testing"
				"github.com/ledgerwatch/turbo-geth/common"
			)

			func TestToken(t *testing.T) {
				
			if b, err := NewToken(common.Address{}, nil); b == nil || err != nil {
				t.Fatalf("binding (%v) nil or error (%v) not nil", b, nil)
			}
		
			}
		