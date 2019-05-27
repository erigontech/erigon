package util

import (
	"strings"

	"github.com/blendlabs/go-exception"
)

var (
	// BooleanTrue represents a true value for a boolean.
	BooleanTrue Boolean = true

	// BooleanFalse represents a false value for a boolean.
	BooleanFalse Boolean = false
)

// Boolean is a type alias for bool that can be unmarshaled from 0|1, true|false etc.
type Boolean bool

// UnmarshalJSON unmarshals the boolean from json.
func (bit *Boolean) UnmarshalJSON(data []byte) error {
	asString := strings.ToLower(string(data))
	if asString == "1" || asString == "true" {
		*bit = true
		return nil
	} else if asString == "0" || asString == "false" {
		*bit = false
		return nil
	} else if len(asString) > 0 && (asString[0] == '"' || asString[0] == '\'') {
		cleaned := String.StripQuotes(asString)
		return bit.UnmarshalJSON([]byte(cleaned))
	}
	return exception.Newf("Boolean unmarshal error: invalid input %s", asString)
}

// AsBool returns the stdlib bool value for the boolean.
func (bit Boolean) AsBool() bool {
	if bit {
		return true
	}
	return false
}
