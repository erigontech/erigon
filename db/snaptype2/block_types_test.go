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

package snaptype2

import (
	"testing"
)

func TestEnumeration(t *testing.T) {

	if Headers.Enum() != Enums.Headers {
		t.Fatal("enum mismatch", Headers, Headers.Enum(), Enums.Headers)
	}

	if Bodies.Enum() != Enums.Bodies {
		t.Fatal("enum mismatch", Bodies, Bodies.Enum(), Enums.Bodies)
	}

	if Transactions.Enum() != Enums.Transactions {
		t.Fatal("enum mismatch", Transactions, Transactions.Enum(), Enums.Transactions)
	}

}

func TestNames(t *testing.T) {

	if Headers.Name() != Enums.Headers.String() {
		t.Fatal("name mismatch", Headers, Headers.Name(), Enums.Headers.String())
	}

	if Bodies.Name() != Enums.Bodies.String() {
		t.Fatal("name mismatch", Bodies, Bodies.Name(), Enums.Bodies.String())
	}

	if Transactions.Name() != Enums.Transactions.String() {
		t.Fatal("name mismatch", Transactions, Transactions.Name(), Enums.Transactions.String())
	}
}
