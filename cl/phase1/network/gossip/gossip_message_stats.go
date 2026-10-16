package gossip

import (
	"strconv"
	"strings"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// gossipValidationCounter counts gossip messages by topic, without the subnet suffix, and
// validation result. Gossipsub validates only the first copy of a message, so every ignore
// here, including "condition" (an extra condition such as a rate limit failed), is a message
// the node neither processes nor forwards.
var gossipValidationCounter = metrics.GetOrCreateCounterVec(
	"caplin_gossip_validation_total",
	[]string{"topic", "result"},
	"Gossip messages validated, by topic and result",
)

type gossipMessageStats struct{}

func newGossipMessageStats() *gossipMessageStats {
	return &gossipMessageStats{}
}

func (s *gossipMessageStats) addAccept(name string)          { s.add(name, "accept") }
func (s *gossipMessageStats) addReject(name string)          { s.add(name, "reject") }
func (s *gossipMessageStats) addIgnore(name string)          { s.add(name, "ignore") }
func (s *gossipMessageStats) addConditionIgnore(name string) { s.add(name, "condition") }

func (s *gossipMessageStats) add(name, result string) {
	tokens := strings.Split(name, "_")
	// if last token is a number, remove it
	if _, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil {
		name = strings.Join(tokens[:len(tokens)-1], "_")
	}
	gossipValidationCounter.WithLabelValues(name, result).Inc()
}
