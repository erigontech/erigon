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
	l2BlockChan           chan types.FullL2Block
	gerUpdatesChan        chan types.GerUpdate
}

func NewTestDatastreamClient(fullL2Blocks []types.FullL2Block, gerUpdates []types.GerUpdate) *TestDatastreamClient {
	client := &TestDatastreamClient{
		fullL2Blocks:   fullL2Blocks,
		gerUpdates:     gerUpdates,
		l2BlockChan:    make(chan types.FullL2Block, 100),
		gerUpdatesChan: make(chan types.GerUpdate, 100),
	}

	return client
}

func (c *TestDatastreamClient) ReadAllEntriesToChannel(bookmark *types.Bookmark) error {
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

func (c *TestDatastreamClient) GetGerUpdatesChan() chan types.GerUpdate {
	return c.gerUpdatesChan
}
func (c *TestDatastreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTimeAtomic
}
func (c *TestDatastreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streamingAtomic
}
