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

func parseBlockNumber(s string) (rpc.BlockNumber, error) {
	var blockNum rpc.BlockNumber
	b, err := json.Marshal(s)
	if err != nil {
		return blockNum, err
	}
	return blockNum, blockNum.UnmarshalJSON(b)
}
