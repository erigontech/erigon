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

type Filters struct {
	mu sync.RWMutex

	headsSubs map[string]chan *types.Header
}

func New(ethBackend core.ApiBackend) *Filters {
	log.Info("rpc filters: subscribing to tg events")

	ff := &Filters{headsSubs: make(map[string]chan *types.Header)}

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

func (ff *Filters) SubscribeNewHeads(out chan *types.Header) string {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := generateSubscriptionID()
	ff.headsSubs[id] = out
	return id
}

func (ff *Filters) Unsubscribe(id string) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.headsSubs, id)
}

func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	if event.Type != remote.Event_HEADER {
		log.Warn("rpc filters: unsupported event type", "type", event.Type)
		return
	}

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
}

func generateSubscriptionID() string {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return fmt.Sprintf("%x", id)
}
