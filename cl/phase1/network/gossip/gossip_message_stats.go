package gossip

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
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

func (s *gossipMessageStats) goPrintStats(ctx context.Context) {
	go func() {
		duration := time.Minute
		ticker := time.NewTicker(duration)
		defer ticker.Stop()
		times := int64(1) // Start at 1 to avoid division by zero

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// logger has internal mutex
				// means need make current mutex lock as short as possible
				s.statsMutex.Lock()
				accepts := maps.Clone(s.accepts)
				rejects := maps.Clone(s.rejects)
				ignores := maps.Clone(s.ignores)
				s.statsMutex.Unlock()

				totalSeconds := float64(times * int64(duration.Seconds()))
				for name, count := range accepts {
					log.Debug("Gossip Message Accepts Stats", "name", name, "count", count, "rate_sec", float64(count)/totalSeconds)
				}
				for name, count := range rejects {
					log.Debug("Gossip Message Rejects Stats", "name", name, "count", count, "rate_sec", float64(count)/totalSeconds)
				}
				for name, count := range ignores {
					log.Debug("Gossip Message Ignores Stats", "name", name, "count", count, "rate_sec", float64(count)/totalSeconds)
				}
				times++
			}
		}
	}()
}
