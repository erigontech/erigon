package filters

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/log"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
)

type Filters struct {
	mu sync.RWMutex

	headsSubs        map[HeadsSubID]chan *types.Header
	pendingLogsSubs  map[PendingLogsSubID]chan types.Logs
	pendingBlockSubs map[PendingBlockSubID]chan *types.Block
}

func New(ethBackend core.ApiBackend) *Filters {
	log.Info("rpc filters: subscribing to tg events")

	ff := &Filters{headsSubs: make(map[HeadsSubID]chan *types.Header), pendingLogsSubs: make(map[PendingLogsSubID]chan types.Logs), pendingBlockSubs: make(map[PendingBlockSubID]chan *types.Block)}

	go func() {
		var err error
		for i := 0; i < 10; i++ {
			err = ethBackend.Subscribe(context.Background(), ff.OnNewEvent)
			if err != nil {
				log.Warn("rpc filters: error subscribing to events", "err", err)
				time.Sleep(time.Second)
			}
		}
	}()

	return ff
}

func (ff *Filters) SubscribeNewHeads(out chan *types.Header) HeadsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := HeadsSubID(generateSubscriptionID())
	ff.headsSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribeHeads(id HeadsSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.headsSubs, id)
}

func (ff *Filters) SubscribePendingLogs(out chan types.Logs) PendingLogsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingLogsSubID(generateSubscriptionID())
	ff.pendingLogsSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribePendingLogs(id PendingLogsSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.pendingLogsSubs, id)
}

func (ff *Filters) SubscribePendingBlock(out chan *types.Block) PendingBlockSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingBlockSubID(generateSubscriptionID())
	ff.pendingBlockSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribePendingBlock(id PendingBlockSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.pendingBlockSubs, id)
}

func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	switch event.Type {
	case remote.Event_HEADER:
		payload := event.Data
		var header types.Header
		err := json.Unmarshal(payload, &header)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("rpc filters, unprocessable payload", "err", err)
		} else {
			for _, v := range ff.headsSubs {
				v <- &header
			}
		}
	case remote.Event_PENDING_LOGS:
		payload := event.Data
		var logs types.Logs
		err := json.Unmarshal(payload, &logs)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("rpc filters, unprocessable payload", "err", err)
		} else {
			for _, v := range ff.pendingLogsSubs {
				v <- logs
			}
		}
	case remote.Event_PENDING_BLOCK:
		payload := event.Data
		var block types.Block
		err := json.Unmarshal(payload, &block)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("rpc filters, unprocessable payload", "err", err)
		} else {
			for _, v := range ff.pendingBlockSubs {
				v <- &block
			}
		}
	default:
		log.Warn("rpc filters: unsupported event type", "type", event.Type)
		return
	}
}

func generateSubscriptionID() SubscriptionID {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return SubscriptionID(fmt.Sprintf("%x", id))
}
