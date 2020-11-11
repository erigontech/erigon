package filters

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type Filters struct {
	mu sync.RWMutex

	headsSubs map[string]chan *types.Header
}

func New(ethBackend ethdb.Backend) *Filters {
	fmt.Println("rpc filters: subscribing to tg events")

	ff := &Filters{headsSubs: make(map[string]chan *types.Header)}

	go func() {
		for {
			ethBackend.Subscribe(ff.OnNewEvent)
		}
	}()

	return ff
}

func (ff *Filters) SubscribeNewHeads(out chan *types.Header) string {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := "testID"
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

	payload := event.Data
	fmt.Println("data received:", string(payload))
	var header types.Header
	err := json.Unmarshal(payload, &header)
	if err != nil {
		fmt.Println("error while unmarhaling header", err)
	} else {
		fmt.Println("got a header #", header.Number)

		for _, v := range ff.headsSubs {
			v <- &header
		}
	}

}
