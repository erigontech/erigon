package collections

import (
	"strconv"
	"strings"
)

// NewSetOfInt creates a new SetOfInt.
func NewSetOfInt(values ...int) SetOfInt {
	set := SetOfInt{}
	for _, v := range values {
		set.Add(v)
	}
	return set
}

// SetOfInt is a type alias for map[int]int
type SetOfInt map[int]bool

// Add adds an element to the set, replaceing a previous value.
func (si SetOfInt) Add(i int) {
	si[i] = true
}

// Remove removes an element from the set.
func (si SetOfInt) Remove(i int) {
	delete(si, i)
}

// Contains returns if the element is in the set.
func (si SetOfInt) Contains(i int) bool {
	_, ok := si[i]
	return ok
}

// Len returns the number of elements in the set.
func (si SetOfInt) Len() int {
	return len(si)
}

// Copy returns a new copy of the set.
func (si SetOfInt) Copy() SetOfInt {
	newSet := NewSetOfInt()
	for key := range si {
		newSet.Add(key)
	}
	return newSet
}

// Union joins two sets together without dupes.
func (si SetOfInt) Union(other SetOfInt) SetOfInt {
	union := NewSetOfInt()
	for k := range si {
		union.Add(k)
	}

	for k := range other {
		union.Add(k)
	}
	return union
}

// Intersect returns shared elements between two sets.
func (si SetOfInt) Intersect(other SetOfInt) SetOfInt {
	intersection := NewSetOfInt()
	for k := range si {
		if other.Contains(k) {
			intersection.Add(k)
		}
	}
	return intersection
}

// Difference returns non-shared elements between two sets.
func (si SetOfInt) Difference(other SetOfInt) SetOfInt {
	difference := NewSetOfInt()
	for k := range si {
		if !other.Contains(k) {
			difference.Add(k)
		}
	}
	for k := range other {
		if !si.Contains(k) {
			difference.Add(k)
		}
	}
	return difference
}

// IsSubsetOf returns if a given set is a complete subset of another set,
// i.e. all elements in target set are in other set.
func (si SetOfInt) IsSubsetOf(other SetOfInt) bool {
	for k := range si {
		if !other.Contains(k) {
			return false
		}
	}
	return true
}

// AsSlice returns the set as a slice.
func (si SetOfInt) AsSlice() []int {
	output := []int{}
	for key := range si {
		output = append(output, key)
	}
	return output
}

// String returns the set as a csv string.
func (si SetOfInt) String() string {
	var values []string
	for _, i := range si.AsSlice() {
		values = append(values, strconv.Itoa(i))
	}

	return strings.Join(values, ", ")
}

// NewSetOfString creates a new SetOfString.
func NewSetOfString(values ...string) SetOfString {
	set := SetOfString{}
	for _, v := range values {
		set.Add(v)
	}
	return set
}

// SetOfString is a set of strings
type SetOfString map[string]bool

// Add adds an element.
func (ss SetOfString) Add(entry string) {
	if _, hasEntry := ss[entry]; !hasEntry {
		ss[entry] = true
	}
}

// Remove deletes an element, returns if the element was in the set.
func (ss SetOfString) Remove(entry string) bool {
	if _, hasEntry := ss[entry]; hasEntry {
		delete(ss, entry)
		return true
	}
	return false
}

// Contains returns if an element is in the set.
func (ss SetOfString) Contains(entry string) bool {
	_, hasEntry := ss[entry]
	return hasEntry
}

// Len returns the length of the set.
func (ss SetOfString) Len() int {
	return len(ss)
}

// Copy returns a new copy of the set.
func (ss SetOfString) Copy() SetOfString {
	newSet := SetOfString{}
	for key := range ss {
		newSet.Add(key)
	}
	return newSet
}

// Union joins two sets together without dupes.
func (ss SetOfString) Union(other SetOfString) SetOfString {
	union := NewSetOfString()
	for k := range ss {
		union.Add(k)
	}

	for k := range other {
		union.Add(k)
	}
	return union
}

// Intersect returns shared elements between two sets.
func (ss SetOfString) Intersect(other SetOfString) SetOfString {
	intersection := NewSetOfString()
	for k := range ss {
		if other.Contains(k) {
			intersection.Add(k)
		}
	}
	return intersection
}

// Difference returns non-shared elements between two sets.
func (ss SetOfString) Difference(other SetOfString) SetOfString {
	difference := NewSetOfString()
	for k := range ss {
		if !other.Contains(k) {
			difference.Add(k)
		}
	}
	for k := range other {
		if !ss.Contains(k) {
			difference.Add(k)
		}
	}
	return difference
}

// IsSubsetOf returns if a given set is a complete subset of another set,
// i.e. all elements in target set are in other set.
func (ss SetOfString) IsSubsetOf(other SetOfString) bool {
	for k := range ss {
		if !other.Contains(k) {
			return false
		}
	}
	return true
}

// AsSlice returns the set as a slice.
func (ss SetOfString) AsSlice() []string {
	output := []string{}
	for key := range ss {
		output = append(output, key)
	}
	return output
}

// String returns the set as a csv string.
func (ss SetOfString) String() string {
	return strings.Join(ss.AsSlice(), ", ")
}
