package beaconevents

import ethevent "github.com/erigontech/erigon/event"

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
	return f.feed.Send(&EventStream{
		Event: StateHead,
		Data:  value,
	})
}
