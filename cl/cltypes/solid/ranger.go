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

package solid

// Define a method with Range method for iteration
type Ranger[T any] interface {
	Range(func(idx int, v T, leng int) bool)
}

// RangeErr is a multi-purpose function that takes a ranger, but not the kind you're thinking of!
// Sadly, this isn't a Megazord-ready Power Ranger, but a Ranger of generic type 'T'.
// The 'T' probably stands for Tyrannosaurus, the coolest Dinozord (change my mind).
func RangeErr[T any](r Ranger[T], fn func(int, T, int) error) (err error) {
	// What does our Power Ranger do, you ask?
	// Well, it ranges over values just like a Power Ranger ranges over Angel Grove.
	r.Range(func(idx int, v T, leng int) bool {
		// This function is like our Power Ranger's special attack.
		// It applies to each enemy (value) in the range. If it fails,
		// it's like a monster has landed a hit on our Power Ranger!
		err = fn(idx, v, leng)
		// If there's an error (i.e., our Power Ranger takes a hit),
		// our Power Ranger falls back (returns false).
		if err != nil {
			return false
		}
		// But if the special attack works (no error), our Power Ranger
		// fights on, moving to the next enemy (returns true)!
		return true
	})
	// After the fight is over (i.e., when we've gone through all the values),
	// our Power Ranger reports back to Zordon (i.e., we return the error, if there was one).
	return
}
