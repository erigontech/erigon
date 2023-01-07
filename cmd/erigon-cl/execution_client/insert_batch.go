package execution_client

import (
	"sync"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

const batchSize = 10000

// InsertBatch is a struct for batching and inserting execution payloads.
type InsertBatch struct {
	ec         *ExecutionClient            // The execution client to use for inserting payloads.
	payloadBuf []*cltypes.ExecutionPayload // A buffer for storing execution payloads before they are inserted.
	mu         sync.Mutex                  // A mutex for synchronizing access to the payload buffer.
}

// NewInsertBatch creates a new InsertBatch struct with the given execution client.
func NewInsertBatch(ec *ExecutionClient) *InsertBatch {
	return &InsertBatch{
		ec:         ec,
		payloadBuf: make([]*cltypes.ExecutionPayload, 0, batchSize),
	}
}

// WriteExecutionPayload adds an execution payload to the payload buffer. If the buffer
// has reached the batch size, the payloads in the buffer are inserted using the
// execution client.
func (b *InsertBatch) WriteExecutionPayload(payload *cltypes.ExecutionPayload) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.payloadBuf = append(b.payloadBuf, payload)
	if len(b.payloadBuf) >= batchSize {
		if err := b.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Flush inserts the execution payloads in the payload buffer using the execution client.
func (b *InsertBatch) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.payloadBuf) == 0 {
		return nil
	}
	if err := b.ec.InsertExecutionPayloads(b.payloadBuf); err != nil {
		return err
	}
	b.payloadBuf = b.payloadBuf[:0] // Clear the payload buffer.
	return nil
}
