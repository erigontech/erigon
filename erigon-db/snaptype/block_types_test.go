// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package snaptype_test

import (
	"testing"

	"github.com/erigontech/erigon-db/snaptype"
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
