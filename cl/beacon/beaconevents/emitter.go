package beaconevents

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

var validTopics = map[string]struct{}{
	"head":                           {},
	"block":                          {},
	"attestation":                    {},
	"voluntary_exit":                 {},
	"bls_to_execution_change":        {},
	"finalized_checkpoint":           {},
	"chain_reorg":                    {},
	"contribution_and_proof":         {},
	"light_client_finality_update":   {},
	"light_client_optimistic_update": {},
	"payload_attributes":             {},
	"*":                              {},
}

type Subscription struct {
	id     string
	topics map[string]struct{}
	cb     func(topic string, item any)
}

type EventName string

type Emitters struct {
	cbs map[string]*Subscription
	mu  sync.RWMutex
}

func NewEmitters() *Emitters {
	return &Emitters{
		cbs: map[string]*Subscription{},
	}
}

func (e *Emitters) Publish(s string, a any) {
	// forward gossip object
	e.mu.Lock()
	values := make([]*Subscription, 0, len(e.cbs))
	for _, v := range e.cbs {
		values = append(values, v)
	}
	e.mu.Unlock()

	for _, v := range values {
		if _, ok := v.topics["*"]; ok {
			go v.cb(s, a)
		} else if _, ok := v.topics[s]; ok {
			go v.cb(s, a)
		}
	}
}

func (e *Emitters) Subscribe(topics []string, cb func(topic string, item any)) (func(), error) {
	subid := uuid.New().String()
	sub := &Subscription{
		id:     subid,
		topics: map[string]struct{}{},
		cb:     cb,
	}
	for _, v := range topics {
		if _, ok := validTopics[v]; !ok {
			return nil, fmt.Errorf("Invalid Topic: %s", v)
		}
		sub.topics[v] = struct{}{}
	}
	e.cbs[subid] = sub
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.cbs, subid)
	}, nil
}
