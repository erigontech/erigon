package filters

import (
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type Filters struct {
}

func New(ethBackend ethdb.Backend) *Filters {
	fmt.Println("rpc filters: subscribing to tg events")

	ff := &Filters{}

	go func() {
		for {
			ethBackend.Subscribe(ff.OnNewEvent)
		}
	}()

	return ff
}

func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	payload := event.Data
	fmt.Println("data received:", string(payload))
	var header types.Header
	err := json.Unmarshal(payload, &header)
	if err != nil {
		fmt.Println("error while unmarhaling header", err)
	} else {
		fmt.Println("got a header #", header.Number)
	}
}
