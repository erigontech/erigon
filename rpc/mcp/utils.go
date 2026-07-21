package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
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

// toJSONIndent pretty-prints raw JSON bytes.
func toJSONIndent(raw json.RawMessage) string {
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return string(raw)
	}
	formatted, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(formatted)
}

// extractURIParam extracts a path parameter from an MCP resource template URI.
// For example, given URI "erigon://address/0xABC/summary" and template prefix
// "erigon://address/" with suffix "/summary", it returns "0xABC". It returns
// "" if the URI does not match the prefix and suffix.
func extractURIParam(uri, prefix, suffix string) string {
	s, ok := strings.CutPrefix(uri, prefix)
	if !ok {
		return ""
	}
	s, ok = strings.CutSuffix(s, suffix)
	if !ok {
		return ""
	}
	return s
}
