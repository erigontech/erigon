package beaconevents

import (
	ethevent "github.com/erigontech/erigon/common/event"
)

type stateFeed struct {
	feed ethevent.Feed
	// head and head_v2 are also sent on feeds of their own, where a subscriber that cannot take one gets
	// ethevent.ErrSubscriberLagged. A lag report there is always about a topic the subscriber asked for; on feed,
	// which carries every state topic, they are dropped silently.
	head, headV2 ethevent.Feed
}

func newStateFeed() *stateFeed {
	return &stateFeed{}
}

func (f *stateFeed) Subscribe(channel chan *EventStream) ethevent.Subscription {
	return f.feed.Subscribe(channel)
}

// SubscribeHead subscribes channel to head events only.
func (f *stateFeed) SubscribeHead(channel chan *EventStream) ethevent.Subscription {
	return f.head.Subscribe(channel)
}

// SubscribeHeadV2 subscribes channel to head_v2 events only.
func (f *stateFeed) SubscribeHeadV2(channel chan *EventStream) ethevent.Subscription {
	return f.headV2.Subscribe(channel)
}

func (f *stateFeed) SendHead(value *HeadData) int {
	e := &EventStream{Event: StateHead, Data: value}
	return f.feed.Send(e) + f.head.Send(e)
}

// TrySendHead and SendHeadV2 do not wait for subscribers. They return how many channels took the event.
func (f *stateFeed) TrySendHead(value *HeadData) int {
	e := &EventStream{Event: StateHead, Data: value}
	return f.feed.TrySend(e) + f.head.TrySendOrFail(e)
}

func (f *stateFeed) SendHeadV2(value *HeadV2Data) int {
	e := &EventStream{Event: StateHeadV2, Data: value}
	return f.feed.TrySend(e) + f.headV2.TrySendOrFail(e)
}

// The node has received a block (from P2P or API) that is successfully imported on the fork-choice on_block handler
func (f *stateFeed) SendBlock(value *BlockData) int {
	return f.feed.Send(&EventStream{
		Event: StateBlock,
		Data:  value,
	})
}

// The node has received a block (from P2P or API) that passes validation rules of the beacon_block topic
func (f *stateFeed) SendBlockGossip(value *BlockGossipData) int {
	return f.feed.Send(&EventStream{
		Event: StateBlockGossip,
		Data:  value,
	})
}

func (f *stateFeed) SendFinalizedCheckpoint(value *FinalizedCheckpointData) int {
	return f.feed.Send(&EventStream{
		Event: StateFinalizedCheckpoint,
		Data:  value,
	})
}

func (f *stateFeed) SendLightClientFinalityUpdate(value *LightClientFinalityUpdateData) int {
	return f.feed.Send(&EventStream{
		Event: StateLightClientFinalityUpdate,
		Data:  value,
	})
}

func (f *stateFeed) SendLightClientOptimisticUpdate(value *LightClientOptimisticUpdateData) int {
	return f.feed.Send(&EventStream{
		Event: StateLightClientOptimisticUpdate,
		Data:  value,
	})
}

func (f *stateFeed) SendChainReorg(value *ChainReorgData) int {
	return f.feed.Send(&EventStream{
		Event: StateChainReorg,
		Data:  value,
	})
}

func (f *stateFeed) SendPayloadAttributes(value *PayloadAttributesData) int {
	return f.feed.Send(&EventStream{
		Event: StatePayloadAttributes,
		Data:  value,
	})
}
