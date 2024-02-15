package service

import (
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon/cl/gossip"
)

const (
	maxSubscribers = 100 // only 100 clients per sentinel
)

type gossipObject struct {
	data []byte // gossip data
	t    string // determine which gossip message we are notifying of
	pid  string // pid is the peer id of the sender
}

type gossipNotifier struct {
	notifiers []chan gossipObject

	mu sync.Mutex
}

func newGossipNotifier() *gossipNotifier {
	return &gossipNotifier{
		notifiers: make([]chan gossipObject, 0, maxSubscribers),
	}
}

func (g *gossipNotifier) notify(t string, data []byte, pid string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, ch := range g.notifiers {
		ch <- gossipObject{
			data: data,
			t:    t,
			pid:  pid,
		}
	}
}

func (g *gossipNotifier) notifyBlob(data []byte, pid string, blobIndex int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, ch := range g.notifiers {
		ch <- gossipObject{
			data: data,
			t:    gossip.TopicNameBlobSidecar(blobIndex),
			pid:  pid,
		}
	}
}

func (g *gossipNotifier) addSubscriber() (chan gossipObject, int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.notifiers) >= maxSubscribers {
		return nil, -1, fmt.Errorf("too many subsribers, try again later")
	}
	ch := make(chan gossipObject, 1<<16)
	g.notifiers = append(g.notifiers, ch)
	return ch, len(g.notifiers) - 1, nil
}

func (g *gossipNotifier) removeSubscriber(id int) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.notifiers) <= id {
		return fmt.Errorf("invalid id, no subscription exist with this id")
	}
	close(g.notifiers[id])
	g.notifiers = append(g.notifiers[:id], g.notifiers[id+1:]...)
	return nil
}
