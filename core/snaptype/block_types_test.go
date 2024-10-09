package snaptype_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/core/snaptype"
)

func TestEnumeration(t *testing.T) {

	if snaptype.Headers.Enum() != snaptype.Enums.Headers {
		t.Fatal("enum mismatch", snaptype.Headers, snaptype.Headers.Enum(), snaptype.Enums.Headers)
	}

	if snaptype.Bodies.Enum() != snaptype.Enums.Bodies {
		t.Fatal("enum mismatch", snaptype.Bodies, snaptype.Bodies.Enum(), snaptype.Enums.Bodies)
	}

	if snaptype.Transactions.Enum() != snaptype.Enums.Transactions {
		t.Fatal("enum mismatch", snaptype.Transactions, snaptype.Transactions.Enum(), snaptype.Enums.Transactions)
	}

}

func TestNames(t *testing.T) {

	if snaptype.Headers.Name() != snaptype.Enums.Headers.String() {
		t.Fatal("name mismatch", snaptype.Headers, snaptype.Headers.Name(), snaptype.Enums.Headers.String())
	}

	if snaptype.Bodies.Name() != snaptype.Enums.Bodies.String() {
		t.Fatal("name mismatch", snaptype.Bodies, snaptype.Bodies.Name(), snaptype.Enums.Bodies.String())
	}

	if snaptype.Transactions.Name() != snaptype.Enums.Transactions.String() {
		t.Fatal("name mismatch", snaptype.Transactions, snaptype.Transactions.Name(), snaptype.Enums.Transactions.String())
	}
}
