package sentinel

import (
	"context"
	"fmt"
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

type TopicName string

const (
	BeaconBlockTopic                 TopicName = "beacon_block"
	LightClientFinalityUpdateTopic   TopicName = "light_client_finality_update"
	LightClientOptimisticUpdateTopic TopicName = "light_client_optimistic_update"
)

type GossipManager struct {
	ch            chan *proto.SubContext
	handler       func(*proto.SubContext) error
	subscriptions map[string]*GossipSubscription
	mu            sync.RWMutex
}

// construct a new gossip manager that will handle packets with the given handlerfunc
func NewGossipManager(
	ctx context.Context,
	handler func(*proto.SubContext) error,
) *GossipManager {
	g := &GossipManager{
		ch:            make(chan *proto.SubContext, 128),
		subscriptions: map[string]*GossipSubscription{},
		handler:       handler,
	}
	go g.run(ctx)
	return g
}

// run loop for gossip manager, not meant to be called
func (g *GossipManager) run(ctx context.Context) {
	do := func(p *proto.SubContext) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("[Gossip] Message Handler Crashed", "err", r)
			}
		}()
		if g.handler != nil {
			err := g.handler(p)
			if err != nil {
				log.Warn("[Gossip] Message Handler Erorr", "err", err)
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case pkt := <-g.ch:
			do(pkt)
		}
	}
}

// closes a specific topic
func (s *GossipManager) CloseTopic(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, ok := s.subscriptions[topic]; ok {
		val.Close()
		delete(s.subscriptions, topic)
	}
}

// closes the gossip manager
func (s *GossipManager) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, val := range s.subscriptions {
		val.Close()
	}
	close(s.ch)
}
func (s *GossipManager) String() string {
	sb := new(strings.Builder)
	s.mu.RLock()
	for _, v := range s.subscriptions {
		sb.Write([]byte(v.topic.String()))
		sb.WriteString("=")
		sb.WriteString(strconv.Itoa(len(v.topic.ListPeers())))
		sb.WriteString(" ")
	}
	s.mu.RUnlock()
	return sb.String()
}

func SentinelGossipSubscribe[T proto.Packet](
	s *Sentinel,
	topic string,
	newcodec func(*pubsub.Subscription) proto.SubCodec,
	opts ...pubsub.TopicOpt,
) error {
	g := s.subManager
	g.mu.Lock()
	defer g.mu.Unlock()
	sub, err := NewGossipSubscription[T](
		s,
		topic,
		g.ch,
		newcodec,
		opts...,
	)
	if err != nil {
		return err
	}
	g.subscriptions[topic] = sub
	return nil
}

func (s *Sentinel) startGossip() (err error) {
	err = SentinelGossipSubscribe[*p2p.SignedBeaconBlockBellatrix](s, s.getTopicByName(BeaconBlockTopic), ssz_snappy.NewSubCodec)
	if err != nil {
		return err
	}
	err = SentinelGossipSubscribe[*p2p.LightClientFinalityUpdate](s, s.getTopicByName(LightClientFinalityUpdateTopic), ssz_snappy.NewSubCodec)
	if err != nil {
		return err
	}
	err = SentinelGossipSubscribe[*p2p.LightClientOptimisticUpdate](s, s.getTopicByName(LightClientOptimisticUpdateTopic), ssz_snappy.NewSubCodec)
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

func (s *Sentinel) getTopicByName(name TopicName) string {
	prefix := "/eth2/4a26c58b"
	return fmt.Sprintf("%s/%s/ssz_snappy", prefix, name)
}
