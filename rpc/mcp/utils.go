package mcp

import (
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon/rpc"
)

// toJSONText converts a value to pretty-printed JSON string
func toJSONText(v any) string {
	if v == nil {
		return "null"
	}
	formatted, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(formatted)
}

// parseBlockNumber parses block number from string
func parseBlockNumber(s string) (rpc.BlockNumber, error) {
	var blockNum rpc.BlockNumber
	err := blockNum.UnmarshalJSON([]byte(`"` + s + `"`))
	return blockNum, err
}
