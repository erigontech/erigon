package beaconevents

import (
	"github.com/erigontech/erigon-lib/log/v3"
	ethevent "github.com/erigontech/erigon/event"
)

type stateFeed struct {
	feed *ethevent.Feed
}

func newStateFeed() *stateFeed {
	return &stateFeed{
		feed: &ethevent.Feed{},
	}
}

func (f *stateFeed) Subscribe(channel chan *EventStream) ethevent.Subscription {
	return f.feed.Subscribe(channel)
}

func (f *stateFeed) SendHead(value *HeadData) int {
	log.Info("[test] send head", "value", value)
	return f.feed.Send(&EventStream{
		Event: StateHead,
		Data:  value,
	})
}
