/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
