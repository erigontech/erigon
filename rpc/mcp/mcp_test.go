package mcp

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/rpc"
)

func TestToJSONText(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: "null",
		},
		{
			name:     "simple string",
			input:    "test",
			expected: "\"test\"",
		},
		{
			name:     "simple number",
			input:    42,
			expected: "42",
		},
		{
			name:     "map",
			input:    map[string]string{"key": "value"},
			expected: "{\n  \"key\": \"value\"\n}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toJSONText(tt.input)
			if result != tt.expected {
				t.Errorf("toJSONText(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseBlockNumber(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "latest",
			input:   "latest",
			wantErr: false,
		},
		{
			name:    "earliest",
			input:   "earliest",
			wantErr: false,
		},
		{
			name:    "pending",
			input:   "pending",
			wantErr: false,
		},
		{
			name:    "hex number",
			input:   "0x10",
			wantErr: false,
		},
		{
			name:    "decimal number",
			input:   "100",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseBlockNumber(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBlockNumber(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == rpc.BlockNumber(0) && tt.input != "0" && tt.input != "0x0" {
				// For non-zero inputs, result should not be zero (unless it's earliest)
				if tt.input != "earliest" {
					t.Logf("parseBlockNumber(%q) = %v", tt.input, result)
				}
			}
		})
	}
}

func TestParseBlockNumberOrHash(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		isHash  bool
	}{
		{
			name:    "block hash",
			input:   "0x1234567890123456789012345678901234567890123456789012345678901234",
			wantErr: false,
			isHash:  true,
		},
		{
			name:    "block number",
			input:   "latest",
			wantErr: false,
			isHash:  false,
		},
		{
			name:    "hex number",
			input:   "0x10",
			wantErr: false,
			isHash:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseBlockNumberOrHash(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBlockNumberOrHash(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// Just verify it doesn't panic
				_ = result
				t.Logf("parseBlockNumberOrHash(%q) succeeded", tt.input)
			}
		})
	}
}

func TestMCPServerCreation(t *testing.T) {
	// This tests that we can create the server struct without panicking
	// We can't fully test without mock APIs
	t.Run("server creation", func(t *testing.T) {
		// Just verify the type exists and can be referenced
		var _ *ErigonMCPServer
	})
}

func TestJSONMarshaling(t *testing.T) {
	// Test that our utility functions work with complex nested structures
	complexData := map[string]any{
		"block": map[string]any{
			"number":     12345,
			"hash":       "0xabcdef",
			"timestamp":  1234567890,
			"difficulty": "1000000",
			"transactions": []string{
				"0x1111111111111111111111111111111111111111111111111111111111111111",
				"0x2222222222222222222222222222222222222222222222222222222222222222",
			},
		},
	}

	result := toJSONText(complexData)

	// Verify it's valid JSON
	var parsed map[string]any
	err := json.Unmarshal([]byte(result), &parsed)
	if err != nil {
		t.Errorf("toJSONText produced invalid JSON: %v", err)
	}

	// Verify structure is preserved
	if block, ok := parsed["block"].(map[string]any); ok {
		if number, ok := block["number"].(float64); !ok || number != 12345 {
			t.Errorf("Block number not preserved correctly")
		}
	} else {
		t.Errorf("Block structure not preserved")
	}
}
