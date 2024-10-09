package polygon

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/core/types"
)

// UnpackLog unpacks log
func UnpackLog(abiObject *abi.ABI, out interface{}, event string, log *types.Log) error {
	if len(log.Data) > 0 {
		if err := abiObject.UnpackIntoInterface(out, event, log.Data); err != nil {
			return err
		}
	}

	var indexed abi.Arguments

	for _, arg := range abiObject.Events[event].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}

	return parseTopics(out, indexed, log.Topics[1:])
}

var (
	reflectHash    = reflect.TypeOf(libcommon.Hash{})
	reflectAddress = reflect.TypeOf(libcommon.Address{})
	reflectBigInt  = reflect.TypeOf(new(big.Int))
)

// parseTopics converts the indexed topic fields into actual log field values.
//
// Note, dynamic types cannot be reconstructed since they get mapped to Keccak256
// hashes as the topic value!
func parseTopics(out interface{}, fields abi.Arguments, topics []libcommon.Hash) error {
	// Sanity check that the fields and topics match up
	if len(fields) != len(topics) {
		return errors.New("topic/field count mismatch")
	}

	// Iterate over all the fields and reconstruct them from topics
	for _, arg := range fields {
		if !arg.Indexed {
			return errors.New("non-indexed field in topic reconstruction")
		}

		field := reflect.ValueOf(out).Elem().FieldByName(capitalise(arg.Name))

		// Try to parse the topic back into the fields based on primitive types
		switch field.Kind() {
		case reflect.Bool:
			if topics[0][length.Hash-1] == 1 {
				field.Set(reflect.ValueOf(true))
			}
		case reflect.Int8:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(int8(num.Int64())))
		case reflect.Int16:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(int16(num.Int64())))
		case reflect.Int32:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(int32(num.Int64())))
		case reflect.Int64:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(num.Int64()))
		case reflect.Uint8:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(uint8(num.Uint64())))
		case reflect.Uint16:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(uint16(num.Uint64())))
		case reflect.Uint32:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(uint32(num.Uint64())))
		case reflect.Uint64:
			num := new(big.Int).SetBytes(topics[0][:])
			field.Set(reflect.ValueOf(num.Uint64()))
		default:
			// Ran out of plain primitive types, try custom types
			switch field.Type() {
			case reflectHash: // Also covers all dynamic types
				field.Set(reflect.ValueOf(topics[0]))
			case reflectAddress:
				var addr libcommon.Address

				copy(addr[:], topics[0][length.Hash-length.Addr:])

				field.Set(reflect.ValueOf(addr))
			case reflectBigInt:
				num := new(big.Int).SetBytes(topics[0][:])
				field.Set(reflect.ValueOf(num))
			default:
				// Ran out of custom types, try the crazies
				switch {
				case arg.Type.T == abi.FixedBytesTy:
					reflect.Copy(field, reflect.ValueOf(topics[0][length.Hash-arg.Type.Size:]))

				default:
					return fmt.Errorf("unsupported indexed type: %v", arg.Type)
				}
			}
		}

		topics = topics[1:]
	}

	return nil
}

// capitalise makes a camel-case string which starts with an upper case character.
func capitalise(input string) string {
	for len(input) > 0 && input[0] == '_' {
		input = input[1:]
	}

	if len(input) == 0 {
		return ""
	}

	return toCamelCase(strings.ToUpper(input[:1]) + input[1:])
}

// toCamelCase converts an under-score string to a camel-case string
func toCamelCase(input string) string {
	toupper := false
	result := ""

	for k, v := range input {
		switch {
		case k == 0:
			result = strings.ToUpper(string(input[0]))
		case toupper:
			result += strings.ToUpper(string(v))
			toupper = false
		case v == '_':
			toupper = true
		default:
			result += string(v)
		}
	}

	return result
}
