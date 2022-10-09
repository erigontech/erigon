package service

import "fmt"

type gossipType int

var (
	// lightclient
	lightClientOptimisticUpdate gossipType = 0
	lightClientFinalityUpdate   gossipType = 1
	// legacy
	beaconBlock gossipType = 2
)

const (
	maxSubscribers = 100 // only 100 lightclients per sentinel
)

var gossipTypes = []gossipType{
	lightClientOptimisticUpdate,
	lightClientFinalityUpdate,
	beaconBlock,
}

type gossipNotifier map[gossipType][]chan interface{}

func newGossipNotifier() gossipNotifier {
	gossipNotifier := make(gossipNotifier)
	for _, t := range gossipTypes {
		gossipNotifier[t] = make([]chan interface{}, 0, maxSubscribers)
	}
	return gossipNotifier
}

func (g gossipNotifier) notify(t gossipType, message interface{}) {
	for _, ch := range g[t] {
		ch <- message
	}
}

func (g gossipNotifier) addSubscriber(t gossipType) (chan interface{}, int, error) {
	if len(g[t]) >= maxSubscribers {
		return nil, -1, fmt.Errorf("too many subsribers, try again later")
	}
	ch := make(chan interface{})
	g[t] = append(g[t], ch)
	return ch, len(g[t]) - 1, nil
}

func (g gossipNotifier) removeSubscriber(t gossipType, id int) error {
	if len(g[t]) <= id {
		return fmt.Errorf("invalid id, no subscription exist with this id")
	}
	close(g[t][id])
	g[t] = append(g[t][:id], g[t][id+1:]...)
	return nil
}
