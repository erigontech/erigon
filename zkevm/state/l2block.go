package state

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

// NewL2BlockEventHandler represent a func that will be called by the
// state when a NewL2BlockEvent is triggered
type NewL2BlockEventHandler func(e NewL2BlockEvent)

// NewL2BlockEvent is a struct provided from the state to the NewL2BlockEventHandler
// when a new l2 block is detected with data related to this new l2 block.
type NewL2BlockEvent struct {
	Block types.Block
}

// PrepareWebSocket allows the RPC to prepare ws
func (s *State) PrepareWebSocket() {
	lastL2Block, err := s.PostgresStorage.GetLastL2Block(context.Background(), nil)
	if errors.Is(err, ErrStateNotSynchronized) {
		lastL2Block = types.NewBlockWithHeader(&types.Header{Number: big.NewInt(0)})
	} else if err != nil {
		log.Fatalf("failed to load the last l2 block: %v", err)
	}
	s.lastL2BlockSeen = *lastL2Block
	go s.monitorNewL2Blocks()
	go s.handleEvents()
}

// RegisterNewL2BlockEventHandler add the provided handler to the list of handlers
// that will be triggered when a new l2 block event is triggered
func (s *State) RegisterNewL2BlockEventHandler(h NewL2BlockEventHandler) {
	log.Info("new l2 block event handler registered")
	s.newL2BlockEventHandlers = append(s.newL2BlockEventHandlers, h)
}

func (s *State) handleEvents() {
	for newL2BlockEvent := range s.newL2BlockEvents {
		if len(s.newL2BlockEventHandlers) == 0 {
			continue
		}

		wg := sync.WaitGroup{}
		for _, handler := range s.newL2BlockEventHandlers {
			wg.Add(1)
			go func(h NewL2BlockEventHandler) {
				defer func() {
					wg.Done()
					if r := recover(); r != nil {
						log.Errorf("failed and recovered in NewL2BlockEventHandler: %v", r)
					}
				}()
				h(newL2BlockEvent)
			}(handler)
		}
		wg.Wait()
	}
}

func (s *State) monitorNewL2Blocks() {
	waitNextCycle := func() {
		time.Sleep(1 * time.Second)
	}

	for {
		if len(s.newL2BlockEventHandlers) == 0 {
			waitNextCycle()
			continue
		}

		lastL2Block, err := s.GetLastL2Block(context.Background(), nil)
		if errors.Is(err, ErrStateNotSynchronized) {
			waitNextCycle()
			continue
		} else if err != nil {
			log.Errorf("failed to get last l2 block while monitoring new blocks: %v", err)
			waitNextCycle()
			continue
		}

		// not updates until now
		if lastL2Block == nil || s.lastL2BlockSeen.NumberU64() >= lastL2Block.NumberU64() {
			waitNextCycle()
			continue
		}

		for bn := s.lastL2BlockSeen.NumberU64() + uint64(1); bn <= lastL2Block.NumberU64(); bn++ {
			block, err := s.GetL2BlockByNumber(context.Background(), bn, nil)
			if err != nil {
				log.Errorf("failed to l2 block while monitoring new blocks: %v", err)
				break
			}

			s.newL2BlockEvents <- NewL2BlockEvent{
				Block: *block,
			}
			log.Infof("new l2 blocks detected, Number %v, Hash %v", block.NumberU64(), block.Hash().String())
			s.lastL2BlockSeen = *block
		}

		// interval to check for new l2 blocks
		waitNextCycle()
	}
}
