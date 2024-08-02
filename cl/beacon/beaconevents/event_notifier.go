package beaconevents

type EventNotifier struct {
	stateFeed     *stateFeed     // block state feed
	operationFeed *operationFeed // block operation feed
}

func NewEventNotifier() *EventNotifier {
	return &EventNotifier{
		operationFeed: newOpFeed(),
		stateFeed:     newStateFeed(),
	}
}

func (e *EventNotifier) State() *stateFeed {
	return e.stateFeed
}

func (e *EventNotifier) Operation() *operationFeed {
	return e.operationFeed
}
