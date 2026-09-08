package gossip

import (
	"strconv"
	"strings"
	"sync"
)

type gossipMessageStats struct {
	accepts    map[string]int64
	rejects    map[string]int64
	ignores    map[string]int64
	statsMutex sync.Mutex
}

func newGossipMessageStats() *gossipMessageStats {
	return &gossipMessageStats{}
}

func (s *gossipMessageStats) addAccept(name string) {
	tokens := strings.Split(name, "_")
	// if last token is a number, remove it
	if _, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil {
		name = strings.Join(tokens[:len(tokens)-1], "_")
	}

	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()
	if s.accepts == nil {
		s.accepts = make(map[string]int64)
	}
	s.accepts[name]++
}

func (s *gossipMessageStats) addReject(name string) {
	tokens := strings.Split(name, "_")
	// if last token is a number, remove it
	if _, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil {
		name = strings.Join(tokens[:len(tokens)-1], "_")
	}

	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()
	if s.rejects == nil {
		s.rejects = make(map[string]int64)
	}
	s.rejects[name]++
}

func (s *gossipMessageStats) addIgnore(name string) {
	tokens := strings.Split(name, "_")
	// if last token is a number, remove it
	if _, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil {
		name = strings.Join(tokens[:len(tokens)-1], "_")
	}

	s.statsMutex.Lock()
	defer s.statsMutex.Unlock()
	if s.ignores == nil {
		s.ignores = make(map[string]int64)
	}
	s.ignores[name]++
}
