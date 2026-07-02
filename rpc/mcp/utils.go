package mcp

import (
	"encoding/json"
	"fmt"
	"strings"

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

// extractURIParam extracts a path parameter from an MCP resource template URI.
// For example, given URI "erigon://address/0xABC/summary" and template prefix
// "erigon://address/" with suffix "/summary", it returns "0xABC".
func extractURIParam(uri, prefix, suffix string) string {
	s := strings.TrimPrefix(uri, prefix)
	if suffix != "" {
		s = strings.TrimSuffix(s, suffix)
	}
	return s
}
