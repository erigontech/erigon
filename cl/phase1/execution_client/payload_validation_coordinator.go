package execution_client

import (
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

type payloadValidationCall struct {
	done   chan struct{}
	status PayloadStatus
	err    error
}

// PayloadValidationCoordinator bounds and coalesces NewPayload calls to one execution client.
type PayloadValidationCoordinator struct {
	engine ExecutionEngine
	slots  chan struct{}
	mu     sync.Mutex
	calls  map[common.Hash]*payloadValidationCall
}

// NewPayloadValidationCoordinator creates a coordinator allowing two concurrent engine calls.
func NewPayloadValidationCoordinator(engine ExecutionEngine) *PayloadValidationCoordinator {
	return &PayloadValidationCoordinator{
		engine: engine,
		slots:  make(chan struct{}, 2),
		calls:  make(map[common.Hash]*payloadValidationCall),
	}
}

// NewPayload validates a payload through the shared concurrency and singleflight gate.
func (c *PayloadValidationCoordinator) NewPayload(
	ctx context.Context,
	key common.Hash,
	payload *cltypes.Eth1Block,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (PayloadStatus, error) {
	c.mu.Lock()
	if call, ok := c.calls[key]; ok {
		c.mu.Unlock()
		return waitForPayloadValidation(ctx, call)
	}
	c.mu.Unlock()

	select {
	case c.slots <- struct{}{}:
	case <-ctx.Done():
		return PayloadStatusNone, ctx.Err()
	}
	c.mu.Lock()
	if call, ok := c.calls[key]; ok {
		c.mu.Unlock()
		<-c.slots
		return waitForPayloadValidation(ctx, call)
	}
	call := &payloadValidationCall{done: make(chan struct{})}
	c.calls[key] = call
	c.mu.Unlock()

	var (
		status     PayloadStatus
		err        error
		panicValue any
	)
	func() {
		defer func() {
			panicValue = recover()
		}()
		status, err = c.engine.NewPayload(ctx, payload, parentBlockRoot, versionedHashes, executionRequestsList)
	}()
	<-c.slots
	if panicValue != nil {
		err = fmt.Errorf("execution client NewPayload panicked: %v", panicValue)
	}
	c.complete(key, call, status, err)
	if panicValue != nil {
		panic(panicValue)
	}
	return status, err
}

func waitForPayloadValidation(ctx context.Context, call *payloadValidationCall) (PayloadStatus, error) {
	select {
	case <-call.done:
		return call.status, call.err
	case <-ctx.Done():
		return PayloadStatusNone, ctx.Err()
	}
}

func (c *PayloadValidationCoordinator) complete(key common.Hash, call *payloadValidationCall, status PayloadStatus, err error) {
	c.mu.Lock()
	call.status = status
	call.err = err
	if c.calls[key] == call {
		delete(c.calls, key)
	}
	close(call.done)
	c.mu.Unlock()
}
