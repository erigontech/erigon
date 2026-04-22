package epbs

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
)

// PayloadAssembler abstracts EL block assembly for the ePBS builder.
// The production implementation is eladapter.Adapter; tests use a mock.
type PayloadAssembler interface {
	AssemblePayload(ctx context.Context, params *builder.Parameters) (payloadId uint64, err error)
	GetPayload(ctx context.Context, payloadId uint64) (*eladapter.AssembledPayload, error)
}

// SpeculativeBuild manages concurrent speculative EL block building.
// It wraps a PayloadAssembler to start builds and retrieve results.
// Multiple builds may be in flight concurrently (each with a different
// payloadId); the MVP strategy is fire-and-forget — no cancellation.
type SpeculativeBuild struct {
	mu     sync.Mutex
	engine PayloadAssembler
	builds map[uint64]struct{} // payloadId → in-flight marker
}

// NewSpeculativeBuild creates a SpeculativeBuild backed by the given assembler.
func NewSpeculativeBuild(engine PayloadAssembler) *SpeculativeBuild {
	return &SpeculativeBuild{
		engine: engine,
		builds: make(map[uint64]struct{}),
	}
}

// StartBuild initiates a new EL block build with the given parameters.
// Returns the payloadId on success, or an error if the EL is busy/failing.
func (s *SpeculativeBuild) StartBuild(ctx context.Context, params *builder.Parameters) (uint64, error) {
	payloadId, err := s.engine.AssemblePayload(ctx, params)
	if err != nil {
		return 0, fmt.Errorf("epbs/speculative: AssemblePayload: %w", err)
	}

	s.mu.Lock()
	s.builds[payloadId] = struct{}{}
	s.mu.Unlock()

	return payloadId, nil
}

// GetResult retrieves the assembled block for the given payloadId.
// Returns nil values (no error) if the build is not yet ready.
func (s *SpeculativeBuild) GetResult(ctx context.Context, payloadId uint64) (*eladapter.AssembledPayload, error) {
	s.mu.Lock()
	_, tracked := s.builds[payloadId]
	s.mu.Unlock()
	if !tracked {
		return nil, fmt.Errorf("epbs/speculative: unknown payloadId %d", payloadId)
	}

	result, err := s.engine.GetPayload(ctx, payloadId)
	if err != nil {
		return nil, fmt.Errorf("epbs/speculative: GetPayload(%d): %w", payloadId, err)
	}
	if result == nil {
		return nil, nil // not ready yet
	}

	// Clean up tracking
	s.mu.Lock()
	delete(s.builds, payloadId)
	s.mu.Unlock()

	return result, nil
}

// PayloadIdToBytes converts a uint64 payload ID to its little-endian byte representation.
// This matches the format used by ExecutionEngine.GetAssembledBlock.
func PayloadIdToBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, id)
	return b
}

// ParentInfo captures the parent context needed for building an execution payload.
type ParentInfo struct {
	Slot          uint64      // Slot of the parent beacon block
	BlockRoot     common.Hash // Beacon block root of the parent
	ExecutionHash common.Hash // EL parent hash to build on
	ShouldExtend  bool        // FULL path (extend) vs EMPTY path (skip)
}
