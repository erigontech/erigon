package sentinel

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/ssz_snappy"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	// overlay parameters
	gossipSubD   = 8  // topic stable mesh target count
	gossipSubDlo = 6  // topic stable mesh low watermark
	gossipSubDhi = 12 // topic stable mesh high watermark

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs

	// fanout ttl
	gossipSubFanoutTTL = 60000000000 // TTL for fanout maps for topics we are not subscribed to but have published to, in nano seconds

	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// misc
	rSubD = 8 // random gossip target
)

// Specifies the prefix for any pubsub topic.
const gossipTopicPrefix = "/eth2/"
const blockSubnetTopicFormat = "/eth2/%x/beacon_block"

type subscriptionManager struct {
	subscribedTopics     map[string]*pubsub.Topic
	runningSubscriptions map[string]*pubsub.Subscription

	mu sync.RWMutex
}

func newSubscriptionManager() subscriptionManager {
	return subscriptionManager{
		subscribedTopics:     make(map[string]*pubsub.Topic),
		runningSubscriptions: make(map[string]*pubsub.Subscription),
	}
}

func (s *subscriptionManager) addTopicSub(k string, t *pubsub.Topic, sub *pubsub.Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribedTopics[k] = t
	s.runningSubscriptions[k] = sub
}

func (s *subscriptionManager) clearTopicSub(k string, t *pubsub.Topic, sub *pubsub.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, ok := s.runningSubscriptions[k]; ok {
		val.Cancel()
	}
	if val, ok := s.subscribedTopics[k]; ok {
		err := val.Close()
		if err != nil {
			return err
		}
	}
	delete(s.runningSubscriptions, k)
	delete(s.subscribedTopics, k)
	return nil
}

func (s *subscriptionManager) String() string {
	sb := new(strings.Builder)
	s.mu.RLock()
	for _, v := range s.subscribedTopics {
		sb.Write([]byte(v.String()))
		sb.WriteString("=")
		sb.WriteString(strconv.Itoa(len(v.ListPeers())))
		sb.WriteString(" ")
	}
	s.mu.RUnlock()
	return sb.String()
}

func (s *Sentinel) startGossip(prefix string) (err error) {
	err = subscribeGossipTopic(s, prefix+"/beacon_block/ssz_snappy", ssz_snappy.NewSubCodec, s.handleBeaconBlockSubscription)
	if err != nil {
		return err
	}
	err = subscribeGossipTopic(s, prefix+"/light_client_finality_update/ssz_snappy", ssz_snappy.NewSubCodec, s.handleFinalitySubscription)
	if err != nil {
		return err
	}
	err = subscribeGossipTopic(s, prefix+"/light_client_optimistic_update/ssz_snappy", ssz_snappy.NewSubCodec, s.handleOptimisticSubscription)
	if err != nil {
		return err
	}

	go func() {
		for {
			log.Info("[Gossip] Network Update", "topic peers", s.subManager.String())
			time.Sleep(30 * time.Second)
		}
	}()
	return nil
}

func subscribeGossipTopic[T proto.Packet](
	s *Sentinel,
	topic string,
	newcodec func(*pubsub.Subscription) proto.SubCodec,
	fn func(ctx *proto.SubContext, v T) error,
	opts ...pubsub.TopicOpt,
) error {
	topicHandle, err := s.pubsub.Join(topic, opts...)
	if err != nil {
		return fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}
	if topicHandle == nil {
		return fmt.Errorf("failed to get topic handle while subscribing")
	}

	subscription, err := topicHandle.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to begin topic %s subscription, err=%s", topic, err)
	}
	s.subManager.addTopicSub(topic, topicHandle, subscription)

	log.Info("[Gossip] began subscription", "topic", subscription.Topic())
	go runSubscriptionHandler(s, subscription, newcodec, fn)
	return nil
}

// currySubHandler uses a func(ctx *proto.StreamContext, dat proto.Packet) error to handle a *pubsub.Subscription
// this allows us to write encoding non specific type safe handler without performance overhead
func runSubscriptionHandler[T proto.Packet](
	s *Sentinel,
	sub *pubsub.Subscription,
	newcodec func(*pubsub.Subscription) proto.SubCodec,
	fn func(ctx *proto.SubContext, v T) error,
) {
	sd := newcodec(sub)
	for {
		var t T
		val := t.Clone().(T)
		ctx, err := sd.Decode(s.ctx, val)
		if err != nil {
			log.Warn("fail to decode gossip packet", "err", err, "topic", ctx.Topic, "pkt", reflect.TypeOf(val))
			continue
		}

		log.Info("[Gossip] received message", "topic", sub.Topic())

		if ctx.Msg.ReceivedFrom == s.host.ID() {
			continue
		}
		err = fn(ctx, val)
		if err != nil {
			log.Warn("failed handling gossip ", "err", err, "topic", ctx.Topic, "pkt", reflect.TypeOf(val))
			continue
		}
		log.Info("[Gossip] Received Subscription", "topic", ctx.Topic)
	}
}

func (s *Sentinel) handleFinalitySubscription(
	ctx *proto.SubContext,
	u *p2p.LightClientFinalityUpdate) error {
	s.state.AddFinalityUpdateEvent(u)
	return nil
}
func (s *Sentinel) handleOptimisticSubscription(
	ctx *proto.SubContext,
	u *p2p.LightClientOptimisticUpdate) error {
	s.state.AddOptimisticUpdateEvent(u)
	return nil
}
func (s *Sentinel) handleBeaconBlockSubscription(
	ctx *proto.SubContext,
	u *p2p.SignedBeaconBlockBellatrix) error {
	log.Info("[Gossip] beacon_block",
		"Slot", u.Block.Slot,
		"Signature", hex.EncodeToString(u.Signature[:]),
		"graffiti", string(u.Block.Body.Graffiti[:]),
		"eth1_blockhash", hex.EncodeToString(u.Block.Body.Eth1Data.BlockHash[:]),
		"stateRoot", hex.EncodeToString(u.Block.StateRoot[:]),
		"parentRoot", hex.EncodeToString(u.Block.ParentRoot[:]),
		"proposerIdx", u.Block.ProposerIndex,
	)
	return nil
}
