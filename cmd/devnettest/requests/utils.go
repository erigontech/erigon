package requests

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

// parseResponse converts any of the rpctest interfaces to a string for readability
func parseResponse(resp interface{}) (string, error) {
	result, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}

// GetNamespaceFromMethod splits a parent method into namespace and the actual method
func GetNamespaceFromMethod(method string) (string, string) {
	parts := strings.SplitN(method, "_", 2)
	return parts[0], parts[1]
}