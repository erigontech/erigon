package beaconevents

type EventNotifier struct {
	//stateFeed     *Feed          // block state feed
	operationFeed *operationFeed // block operation feed
}

func NewEventNotifier() *EventNotifier {
	// register all event topics
	/*stateFeed := newFeed(map[EventTopic]interface{}{
		// todo
		StateHead:                HeadData{},
		StateBlock:               BlockData{},
		StateBlockGossip:         BlockGossipData{},
		StateFinalizedCheckpoint: FinalizedCheckpointData{},
		StateChainReorg:          ChainReorgData{},
		StateFinalityUpdate:      LightClientFinalityUpdateData{},
		StateOptimisticUpdate:    LightClientOptimisticUpdateData{},
		StatePayloadAttributes:   PayloadAttributesData{},
	})*/

	return &EventNotifier{
		operationFeed: newOpFeed(),
		//stateFeed:     stateFeed,
	}
}

/*
	func (e *EventNotifier) State() *Feed {
		return e.stateFeed
	}
*/
func (e *EventNotifier) Operation() *operationFeed {
	return e.operationFeed
}
