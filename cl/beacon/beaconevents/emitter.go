package beaconevents

import (
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type Subscription struct {
	id     string
	topics map[string]struct{}
	cb     func(topic string, item any)
}

type EventName string

// Emitters creates pub/sub connection
type Emitters struct {
	cbs map[string]*Subscription
	mu  sync.RWMutex
}

func NewEmitters() *Emitters {
	return &Emitters{
		cbs: map[string]*Subscription{},
	}
}

// publish to all subscribers. each callback is run in a separate goroutine
func (e *Emitters) Publish(s string, a any) {
	// forward gossip object
	e.mu.Lock()
	values := make([]*Subscription, 0, len(e.cbs))
	for _, v := range e.cbs {
		values = append(values, v)
	}
	e.mu.Unlock()

	egg := errgroup.Group{}
	for idx := range values {
		v := values[idx]
		exec := func() error { v.cb(s, a); return nil }
		if _, ok := v.topics["*"]; ok {
			egg.Go(exec)
		} else if _, ok := v.topics[s]; ok {
			egg.Go(exec)
		}
	}
	egg.Wait()
}

// subscribe with callback. call the returned cancelfunc to unregister the callback
// publish will block until all callbacks for the message are resolved
func (e *Emitters) Subscribe(topics []string, cb func(topic string, item any)) (func(), error) {
	subid := uuid.New().String()
	sub := &Subscription{
		id:     subid,
		topics: map[string]struct{}{},
		cb:     cb,
	}
	for _, v := range topics {
		sub.topics[v] = struct{}{}
	}
	e.cbs[subid] = sub
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.cbs, subid)
	}, nil
}
