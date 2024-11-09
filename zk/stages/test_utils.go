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
	stopReadingToChannel  atomic.Bool
	progress              atomic.Uint64
	entriesChan           chan interface{}
	errChan               chan error
	isStarted             bool
}

func NewTestDatastreamClient(fullL2Blocks []types.FullL2Block, gerUpdates []types.GerUpdate) *TestDatastreamClient {
	client := &TestDatastreamClient{
		fullL2Blocks: fullL2Blocks,
		gerUpdates:   gerUpdates,
		entriesChan:  make(chan interface{}, 1000),
		errChan:      make(chan error, 100),
	}

	return client
}

func (c *TestDatastreamClient) ReadAllEntriesToChannel() error {
	c.streamingAtomic.Store(true)
	defer c.streamingAtomic.Swap(false)

	for i := range c.fullL2Blocks {
		c.entriesChan <- &c.fullL2Blocks[i]
	}
	for i := range c.gerUpdates {
		c.entriesChan <- &c.gerUpdates[i]
	}

	c.entriesChan <- nil // needed to stop processing

	for {
		if c.stopReadingToChannel.Load() {
			break
		}
	}

	return nil
}

func (c *TestDatastreamClient) RenewEntryChannel() {
}

func (c *TestDatastreamClient) StopReadingToChannel() {
	c.stopReadingToChannel.Store(true)
}

func (c *TestDatastreamClient) GetEntryChan() *chan interface{} {
	return &c.entriesChan
}

func (c *TestDatastreamClient) GetErrChan() chan error {
	return c.errChan
}

func (c *TestDatastreamClient) GetL2BlockByNumber(blockNum uint64) (*types.FullL2Block, error) {
	for _, l2Block := range c.fullL2Blocks {
		if l2Block.L2BlockNumber == blockNum {
			return &l2Block, nil
		}
	}

	return nil, nil
}

func (c *TestDatastreamClient) GetLatestL2Block() (*types.FullL2Block, error) {
	if len(c.fullL2Blocks) == 0 {
		return nil, nil
	}
	return &c.fullL2Blocks[len(c.fullL2Blocks)-1], nil
}

func (c *TestDatastreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTimeAtomic
}

func (c *TestDatastreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}

func (c *TestDatastreamClient) ReadBatches(start uint64, end uint64) ([][]*types.FullL2Block, error) {
	return nil, nil
}

func (c *TestDatastreamClient) Start() error {
	c.isStarted = true
	return nil
}

func (c *TestDatastreamClient) Stop() {
	c.isStarted = false
}
