package hermez_db

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
)

func TestSplitKey(t *testing.T) {
	scenarios := []struct {
		input       []byte
		outputL1    uint64
		outputBatch uint64
		hasError    bool
	}{
		{
			append(make([]byte, 8), make([]byte, 8)...),
			0, 0, false,
		},
		{
			append([]byte{0, 0, 0, 0, 0, 0, 0, 1}, make([]byte, 8)...),
			1, 0, false,
		},
		{
			append(make([]byte, 8), []byte{0, 0, 0, 0, 0, 0, 0, 1}...),
			0, 1, false,
		},
		{
			make([]byte, 15),
			0, 0, true,
		},
	}

	for _, tt := range scenarios {
		t.Run(fmt.Sprintf("Input: %v", tt.input), func(t *testing.T) {
			l1, batch, err := SplitKey(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if l1 != tt.outputL1 || batch != tt.outputBatch {
					t.Errorf("got (%d, %d), want (%d, %d)", l1, batch, tt.outputL1, tt.outputBatch)
				}
			}
		})
	}
}

func TestConcatGerKey(t *testing.T) {
	tests := map[string]struct {
		blockNo     uint64
		l1BlockHash common.Hash
		expected    []byte
	}{
		"Normal Case": {
			blockNo:     12345,
			l1BlockHash: common.HexToHash("0xbeef"),
			expected:    append(Uint64ToBytes(12345), common.HexToHash("0xbeef").Bytes()...),
		},
		"Zero l1BlockHash": {
			blockNo:     12345,
			l1BlockHash: common.Hash{},
			expected:    Uint64ToBytes(12345),
		},
		"Extreme Block Number": {
			blockNo:     ^uint64(0),
			l1BlockHash: common.HexToHash("0xbeef"),
			expected:    append(Uint64ToBytes(^uint64(0)), common.HexToHash("0xbeef").Bytes()...),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ConcatGerKey(tt.blockNo, tt.l1BlockHash); !bytes.Equal(got, tt.expected) {
				t.Errorf("ConcatGerKey() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSplitGerKey(t *testing.T) {
	beefHash := common.HexToHash("0xbeef")

	tests := map[string]struct {
		input       []byte
		wantBlockNo uint64
		wantHash    *common.Hash
		wantErr     bool
	}{
		"Normal Case 40 Bytes": {
			input:       append(Uint64ToBytes(12345), common.HexToHash("0xbeef").Bytes()...),
			wantBlockNo: 12345,
			wantHash:    &beefHash,
		},
		"Normal Case 8 Bytes": {
			input:       Uint64ToBytes(12345),
			wantBlockNo: 12345,
		},
		"Incorrect Data Length": {
			input:   []byte{0x00, 0x01},
			wantErr: true,
		},
		"Extreme Block Number": {
			input:       append(Uint64ToBytes(^uint64(0)), common.HexToHash("0xbeef").Bytes()...),
			wantBlockNo: ^uint64(0), // max uint64
			wantHash:    &beefHash,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			gotBlockNo, gotHash, err := SplitGerKey(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("SplitGerKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotBlockNo != tt.wantBlockNo {
				t.Errorf("SplitGerKey() gotBlockNo = %v, want %v", gotBlockNo, tt.wantBlockNo)
			}
			if !hashesEqual(gotHash, tt.wantHash) {
				t.Errorf("SplitGerKey() gotHash = %v, want %v", gotHash, tt.wantHash)
			}
		})
	}
}

func hashesEqual(a, b *common.Hash) bool {
	if a == nil && b == nil {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}
