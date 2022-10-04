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

type GossipTopic struct {
	Name     TopicName
	Codec    func(*pubsub.Subscription, *pubsub.Topic) proto.GossipCodec
	Typ      proto.Packet
	CodecStr string
}

var BeaconBlockSsz = GossipTopic{
	Name:     BeaconBlockTopic,
	Typ:      &p2p.SignedBeaconBlockBellatrix{},
	Codec:    ssz_snappy.NewGossipCodec,
	CodecStr: "ssz_snappy",
}
var LightClientFinalityUpdateSsz = GossipTopic{
	Name:     LightClientFinalityUpdateTopic,
	Typ:      &p2p.LightClientFinalityUpdate{},
	Codec:    ssz_snappy.NewGossipCodec,
	CodecStr: "ssz_snappy",
}
var LightClientOptimisticUpdateSsz = GossipTopic{
	Name:     LightClientOptimisticUpdateTopic,
	Typ:      &p2p.LightClientOptimisticUpdate{},
	Codec:    ssz_snappy.NewGossipCodec,
	CodecStr: "ssz_snappy",
}

type GossipManager struct {
	ch            chan *proto.GossipContext
	handler       func(*proto.GossipContext) error
	subscriptions map[string]*GossipSubscription
	mu            sync.RWMutex
}

// construct a new gossip manager that will handle packets with the given handlerfunc
func NewGossipManager(
	ctx context.Context,
) *GossipManager {
	g := &GossipManager{
		ch:            make(chan *proto.GossipContext, 1),
		subscriptions: map[string]*GossipSubscription{},
	}
	return g
}

func (s *GossipManager) Recv() <-chan *proto.GossipContext {
	return s.ch
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

// get a specific topic
func (s *GossipManager) GetSubscription(topic string) (*GossipSubscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.subscriptions[topic]; ok {
		return val, true
	}
	return nil, false
}

// starts listening to a specific topic (forwarding its messages to the gossip manager channel)
func (s *GossipManager) ListenTopic(topic string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.subscriptions[topic]; ok {
		return val.Listen()
	}
	return nil
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
func (s *Sentinel) SubscribeGossip(topic GossipTopic, opts ...pubsub.TopicOpt) (sub *GossipSubscription, err error) {
	sub = &GossipSubscription{
		gossip_topic: topic,
		ch:           s.subManager.ch,
		host:         s.host.ID(),
		ctx:          s.ctx,
	}
	path := s.getTopic(topic)
	sub.topic, err = s.pubsub.Join(path, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s, err=%w", path, err)
	}
	s.subManager.mu.Lock()
	s.subManager.subscriptions[path] = sub
	s.subManager.mu.Unlock()
	return sub, nil
}

func (s *Sentinel) LogTopicPeers() {
	log.Info("[Gossip] Network Update", "topic peers", s.subManager.String())
}

func (s *Sentinel) getTopic(topic GossipTopic) string {
	o, err := s.getCurrentForkChoice()
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	return fmt.Sprintf("/eth2/%x/%s/%s", o, topic.Name, topic.CodecStr)

}

// TODO: this should check the current block i believe?
func (s *Sentinel) getCurrentForkChoice() (o [4]byte, err error) {
	bt := (*[4]byte)(s.cfg.BeaconConfig.BellatrixForkVersion[:4])
	fd := &p2p.ForkData{
		CurrentVersion:        *bt,
		GenesisValidatorsRoot: p2p.Root(s.cfg.GenesisConfig.GenesisValidatorRoot),
	}
	root, err := fd.HashTreeRoot()
	if err != nil {
		return [4]byte{}, err
	}
	copy(o[:], root[:4])
	return
}
