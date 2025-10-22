package network

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

type gossipMessageStats struct {
	accepts    map[string]int64
	rejects    map[string]int64
	statsMutex sync.Mutex
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

func (s *gossipMessageStats) goPrintStats() {
	go func() {
		duration := time.Minute
		ticker := time.NewTicker(duration)
		defer ticker.Stop()
		times := int64(0)
		for range ticker.C {
			s.statsMutex.Lock()
			for name, count := range s.accepts {
				log.Debug("Gossip Message Accepts Stats", "name", name, "count", count, "rate_sec", float64(count)/float64(times*int64(duration.Seconds())))
			}
			for name, count := range s.rejects {
				log.Debug("Gossip Message Rejects Stats", "name", name, "count", count, "rate_sec", float64(count)/float64(times*int64(duration.Seconds())))
			}
			s.statsMutex.Unlock()
			times++
		}
	}()
}
