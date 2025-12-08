package stateless

import (
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
)

// MockHeaderReader is a mock implementation of HeaderReader for testing.
type mockHeaderReader struct {
	headers map[common.Hash]*types.Header
}

func (m *mockHeaderReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return m.headers[hash]
}

func newMockHeaderReader() *mockHeaderReader {
	return &mockHeaderReader{
		headers: make(map[common.Hash]*types.Header),
	}
}

func (m *mockHeaderReader) addHeader(header *types.Header) {
	m.headers[header.Hash()] = header
}

func TestValidateWitnessPreState_Success(t *testing.T) {
	// Create test headers.
	parentStateRoot := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

	parentHeader := &types.Header{
		Number:     big.NewInt(99),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Root:       parentStateRoot,
	}

	// Use the actual hash of the parent header.
	parentHash := parentHeader.Hash()

	contextHeader := &types.Header{
		Number:     big.NewInt(100),
		ParentHash: parentHash,
		Root:       common.HexToHash("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"),
	}

	// Set up mock header reader.
	mockReader := newMockHeaderReader()
	mockReader.addHeader(parentHeader)

	// Create witness with matching pre-state root.
	witness := &Witness{
		context: contextHeader,
		Headers: []*types.Header{parentHeader}, // First header should be parent.
		Codes:   make(map[string]struct{}),
		State:   make(map[string]struct{}),
	}

	// Test validation - should succeed.
	err := ValidateWitnessPreState(witness, mockReader)
	if err != nil {
		t.Errorf("Expected validation to succeed, but got error: %v", err)
	}
}

func TestValidateWitnessPreState_StateMismatch(t *testing.T) {
	// Create test headers with mismatched state roots.
	parentStateRoot := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	mismatchedStateRoot := common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999")

	parentHeader := &types.Header{
		Number:     big.NewInt(99),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Root:       parentStateRoot,
	}

	// Use the actual hash of the parent header.
	parentHash := parentHeader.Hash()

	contextHeader := &types.Header{
		Number:     big.NewInt(100),
		ParentHash: parentHash,
		Root:       common.HexToHash("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"),
	}

	// Create witness header with mismatched state root.
	witnessParentHeader := &types.Header{
		Number:     big.NewInt(99),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Root:       mismatchedStateRoot, // Different from actual parent.
	}

	// Set up mock header reader.
	mockReader := newMockHeaderReader()
	mockReader.addHeader(parentHeader)

	// Create witness with mismatched pre-state root.
	witness := &Witness{
		context: contextHeader,
		Headers: []*types.Header{witnessParentHeader}, // Mismatched parent header.
		Codes:   make(map[string]struct{}),
		State:   make(map[string]struct{}),
	}

	// Test validation - should fail.
	err := ValidateWitnessPreState(witness, mockReader)
	if err == nil {
		t.Error("Expected validation to fail due to state root mismatch, but it succeeded")
	}

	expectedError := "witness pre-state root mismatch"
	if err != nil && len(err.Error()) > 0 {
		if err.Error()[:len(expectedError)] != expectedError {
			t.Errorf("Expected error message to start with '%s', but got: %v", expectedError, err)
		}
	}
}

func TestValidateWitnessPreState_EdgeCases(t *testing.T) {
	mockReader := newMockHeaderReader()

	// Test case 1: Nil witness.
	t.Run("NilWitness", func(t *testing.T) {
		err := ValidateWitnessPreState(nil, mockReader)
		if err == nil {
			t.Error("Expected validation to fail for nil witness")
		}
		if err.Error() != "witness is nil" {
			t.Errorf("Expected error 'witness is nil', got: %v", err)
		}
	})

	// Test case 2: Witness with no headers.
	t.Run("NoHeaders", func(t *testing.T) {
		witness := &Witness{
			context: &types.Header{Number: big.NewInt(100)},
			Headers: []*types.Header{}, // Empty headers.
			Codes:   make(map[string]struct{}),
			State:   make(map[string]struct{}),
		}

		err := ValidateWitnessPreState(witness, mockReader)
		if err == nil {
			t.Error("Expected validation to fail for witness with no headers")
		}
		if err.Error() != "witness has no headers" {
			t.Errorf("Expected error 'witness has no headers', got: %v", err)
		}
	})

	// Test case 3: Witness with nil context header.
	t.Run("NilContextHeader", func(t *testing.T) {
		witness := &Witness{
			context: nil, // Nil context header.
			Headers: []*types.Header{
				{
					Number: big.NewInt(99),
					Root:   common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
				},
			},
			Codes: make(map[string]struct{}),
			State: make(map[string]struct{}),
		}

		err := ValidateWitnessPreState(witness, mockReader)
		if err == nil {
			t.Error("Expected validation to fail for witness with nil context header")
		}
		if err.Error() != "witness context header is nil" {
			t.Errorf("Expected error 'witness context header is nil', got: %v", err)
		}
	})

	// Test case 4: Parent header not found.
	t.Run("ParentNotFound", func(t *testing.T) {
		contextHeader := &types.Header{
			Number:     big.NewInt(100),
			ParentHash: common.HexToHash("0xnonexistent1234567890abcdef1234567890abcdef1234567890abcdef123456"),
			Root:       common.HexToHash("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"),
		}

		witness := &Witness{
			context: contextHeader,
			Headers: []*types.Header{
				{
					Number: big.NewInt(99),
					Root:   common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
				},
			},
			Codes: make(map[string]struct{}),
			State: make(map[string]struct{}),
		}

		// Don't add parent header to mock reader - it won't be found.
		err := ValidateWitnessPreState(witness, mockReader)
		if err == nil {
			t.Error("Expected validation to fail when parent header is not found")
		}

		expectedError := "parent block header not found"
		if err != nil && len(err.Error()) > len(expectedError) {
			if err.Error()[:len(expectedError)] != expectedError {
				t.Errorf("Expected error message to start with '%s', but got: %v", expectedError, err)
			}
		}
	})
}

func TestValidateWitnessPreState_MultipleHeaders(t *testing.T) {
	// Test witness with multiple headers (realistic scenario).
	parentStateRoot := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	grandParentStateRoot := common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555")

	grandParentHeader := &types.Header{
		Number:     big.NewInt(98),
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Root:       grandParentStateRoot,
	}

	// Use the actual hash of the grandparent header.
	grandParentHash := grandParentHeader.Hash()

	parentHeader := &types.Header{
		Number:     big.NewInt(99),
		ParentHash: grandParentHash,
		Root:       parentStateRoot,
	}

	// Use the actual hash of the parent header.
	parentHash := parentHeader.Hash()

	contextHeader := &types.Header{
		Number:     big.NewInt(100),
		ParentHash: parentHash,
		Root:       common.HexToHash("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"),
	}

	// Set up mock header reader.
	mockReader := newMockHeaderReader()
	mockReader.addHeader(parentHeader)
	mockReader.addHeader(grandParentHeader)

	// Create witness with multiple headers (parent should be first).
	witness := &Witness{
		context: contextHeader,
		Headers: []*types.Header{parentHeader, grandParentHeader}, // Multiple headers.
		Codes:   make(map[string]struct{}),
		State:   make(map[string]struct{}),
	}

	// Test validation - should succeed (only first header matters for validation).
	err := ValidateWitnessPreState(witness, mockReader)
	if err != nil {
		t.Errorf("Expected validation to succeed with multiple headers, but got error: %v", err)
	}
}
