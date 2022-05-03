package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// HexToInt converts a hex string to a type uint64
func HexToInt(hexStr string) uint64 {
	// Remove the 0x prefix
	cleaned := strings.ReplaceAll(hexStr, "0x", "")

	result, _ := strconv.ParseUint(cleaned, 16, 64)
	return result
}

// ParseResponse converts any of the rpctest interfaces to a string for readability
func ParseResponse(resp interface{}) (string, error) {
	result, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}

// NamespaceAndSubMethodFromMethod splits a parent method into namespace and the actual method
func NamespaceAndSubMethodFromMethod(method string) (string, string, error) {
	parts := strings.SplitN(method, "_", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid string to split")
	}
	return parts[0], parts[1], nil
}
