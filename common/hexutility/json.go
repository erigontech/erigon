package hexutility

import (
	"encoding/json"
	"errors"
	"reflect"
)

// UnmarshalFixedJSON decodes the input as a string with 0x prefix. The length of out
// determines the required input length. This function is commonly used to implement the
// UnmarshalJSON method for fixed-size types.
func UnmarshalFixedJSON(typ reflect.Type, input, out []byte) error {
	if !isString(input) {
		return &json.UnmarshalTypeError{Value: "non-string", Type: typ}
	}
	return wrapTypeError(UnmarshalFixedText(typ.String(), input[1:len(input)-1], out), typ)
}

func isString(input []byte) bool {
	return len(input) >= 2 && input[0] == '"' && input[len(input)-1] == '"'
}

func wrapTypeError(err error, typ reflect.Type) error {
	var dec *decError
	if errors.As(err, &dec) {
		return &json.UnmarshalTypeError{Value: err.Error(), Type: typ}
	}
	return err
}
