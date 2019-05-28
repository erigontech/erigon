package collections

import (
	"strings"

	"github.com/blendlabs/go-util"
)

// StringArray is a type alias for []string with some helper methods.
type StringArray []string

// Contains returns if the given string is in the array.
func (sa StringArray) Contains(elem string) bool {
	for _, arrayElem := range sa {
		if arrayElem == elem {
			return true
		}
	}
	return false
}

// ContainsLower returns true if the `elem` is in the StringArray, false otherwise.
func (sa StringArray) ContainsLower(elem string) bool {
	for _, arrayElem := range sa {
		if strings.ToLower(arrayElem) == elem {
			return true
		}
	}
	return false
}

// GetByLower returns an element from the array that matches the input.
func (sa StringArray) GetByLower(elem string) string {
	for _, arrayElem := range sa {
		if strings.ToLower(arrayElem) == elem {
			return arrayElem
		}
	}
	return util.StringEmpty
}
