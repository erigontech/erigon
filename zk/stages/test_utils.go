package stages

import (
	"sync/atomic"

	"github.com/ledgerwatch/erigon/zk/datastream/types"
)

type TestDatastreamClient struct {
	fullL2Blocks          []types.FullL2Block
	gerUpdates            []types.GerUpdate
	lastWrittenTimeAtomic atomic.Int64
	streamingAtomic       atomic.Bool
	progress              atomic.Uint64
	l2BlockChan           chan types.FullL2Block
	l2TxChan              chan types.L2TransactionProto
	gerUpdatesChan        chan types.GerUpdate
	errChan               chan error
	batchStartChan        chan types.BatchStart
	batchEndChan          chan types.BatchEnd
}

func NewTestDatastreamClient(fullL2Blocks []types.FullL2Block, gerUpdates []types.GerUpdate) *TestDatastreamClient {
	client := &TestDatastreamClient{
		fullL2Blocks:   fullL2Blocks,
		gerUpdates:     gerUpdates,
		l2BlockChan:    make(chan types.FullL2Block, 100),
		gerUpdatesChan: make(chan types.GerUpdate, 100),
		errChan:        make(chan error, 100),
		batchStartChan: make(chan types.BatchStart, 100),
	}

	return client
}

func (c *TestDatastreamClient) EnsureConnected() (bool, error) {
	return true, nil
}

func (c *TestDatastreamClient) ReadAllEntriesToChannel() error {
	c.streamingAtomic.Store(true)

	for _, block := range c.fullL2Blocks {
		c.l2BlockChan <- block
	}
	for _, update := range c.gerUpdates {
		c.gerUpdatesChan <- update
	}

	return nil
}

func (c *TestDatastreamClient) GetL2BlockChan() chan types.FullL2Block {
	return c.l2BlockChan
}

func (c *TestDatastreamClient) GetL2TxChan() chan types.L2TransactionProto {
	return c.l2TxChan
}

func (c *TestDatastreamClient) GetGerUpdatesChan() chan types.GerUpdate {
	return c.gerUpdatesChan
}

func (c *TestDatastreamClient) GetErrChan() chan error {
	return c.errChan
}

func (c *TestDatastreamClient) GetBatchStartChan() chan types.BatchStart {
	return c.batchStartChan
}

func (c *TestDatastreamClient) GetBatchEndChan() chan types.BatchEnd {
	return c.batchEndChan
}

func (c *TestDatastreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTimeAtomic
}
func (c *TestDatastreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streamingAtomic
}
func (c *TestDatastreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}
