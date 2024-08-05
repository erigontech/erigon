package beaconevents

type EventEmitter struct {
	stateFeed     *stateFeed     // block state feed
	operationFeed *operationFeed // block operation feed
}

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		operationFeed: newOpFeed(),
		stateFeed:     newStateFeed(),
	}
}

func (e *EventEmitter) State() *stateFeed {
	return e.stateFeed
}

func (e *EventEmitter) Operation() *operationFeed {
	return e.operationFeed
}
