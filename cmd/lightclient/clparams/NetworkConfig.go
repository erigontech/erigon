package clparams

import (
	"time"
)

type NetworkConfig struct {
	GossipMaxSize                   uint64        `json:"gossip_max_size"`                    // The maximum allowed size of uncompressed gossip messages.
	MaxRequestBlocks                uint64        `json:"max_request_blocks"`                 // Maximum number of blocks in a single request
	MinEpochsForBlockRequests       uint64        `json:"min_epochs_for_block_requests"`      // The minimum epoch range over which a node must serve blocks
	MaxChunkSize                    uint64        `json:"max_chunk_size"`                     // The maximum allowed size of uncompressed req/resp chunked responses.
	AttestationSubnetCount          uint64        `json:"attestation_subnet_count"`           // The number of attestation subnets used in the gossipsub protocol.
	TtfbTimeout                     time.Duration `json:"ttfbt_timeout"`                      // The maximum time to wait for first byte of request response (time-to-first-byte).
	RespTimeout                     time.Duration `json:"resp_timeout"`                       // The maximum time for complete response transfer.
	AttestationPropagationSlotRange uint64        `json:"attestation_propagation_slot_range"` // The maximum number of slots during which an attestation can be propagated.
	MaximumGossipClockDisparity     time.Duration `json:"maximum_gossip_clock_disparity"`     // The maximum milliseconds of clock disparity assumed between honest nodes.
	MessageDomainInvalidSnappy      [4]byte       `json:"message_domain_invalid_snappy"`      // 4-byte domain for gossip message-id isolation of invalid snappy messages
	MessageDomainValidSnappy        [4]byte       `json:"message_domain_valid_snappy"`        // 4-byte domain for gossip message-id isolation of valid snappy messages

	// DiscoveryV5 Config
	Eth2key                    string // ETH2Key is the ENR key of the Ethereum consensus object in an enr.
	AttSubnetKey               string // AttSubnetKey is the ENR key of the subnet bitfield in the enr.
	SyncCommsSubnetKey         string // SyncCommsSubnetKey is the ENR key of the sync committee subnet bitfield in the enr.
	MinimumPeersInSubnetSearch uint64 // PeersInSubnetSearch is the required amount of peers that we need to be able to lookup in a subnet search.

	ContractDeploymentBlock uint64 // the eth1 block in which the deposit contract is deployed.
	bootNodes               []string
}

var mainnetNetworkConfig = &NetworkConfig{
	GossipMaxSize:                   1 << 20, // 1 MiB
	MaxChunkSize:                    1 << 20, // 1 MiB
	AttestationSubnetCount:          64,
	AttestationPropagationSlotRange: 32,
	MaxRequestBlocks:                1 << 10, // 1024
	TtfbTimeout:                     5 * time.Second,
	RespTimeout:                     10 * time.Second,
	MaximumGossipClockDisparity:     500 * time.Millisecond,
	MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
	MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
	Eth2key:                         "eth2",
	AttSubnetKey:                    "attnets",
	SyncCommsSubnetKey:              "syncnets",
	MinimumPeersInSubnetSearch:      20,
	ContractDeploymentBlock:         11184524,
	bootNodes:                       BootstrapNodes[Mainnet],
}

var sepoliaNetworkConfig = &NetworkConfig{
	GossipMaxSize:                   1 << 20, // 1 MiB
	MaxChunkSize:                    1 << 20, // 1 MiB
	AttestationSubnetCount:          64,
	AttestationPropagationSlotRange: 32,
	MaxRequestBlocks:                1 << 10, // 1024
	TtfbTimeout:                     5 * time.Second,
	RespTimeout:                     10 * time.Second,
	MaximumGossipClockDisparity:     500 * time.Millisecond,
	MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
	MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
	Eth2key:                         "eth2",
	AttSubnetKey:                    "attnets",
	SyncCommsSubnetKey:              "syncnets",
	MinimumPeersInSubnetSearch:      20,
	ContractDeploymentBlock:         1273020,
	bootNodes:                       BootstrapNodes[Sepolia],
}

var goerliNetworkConfig = &NetworkConfig{
	GossipMaxSize:                   1 << 20, // 1 MiB
	MaxChunkSize:                    1 << 20, // 1 MiB
	AttestationSubnetCount:          64,
	AttestationPropagationSlotRange: 32,
	MaxRequestBlocks:                1 << 10, // 1024
	TtfbTimeout:                     5 * time.Second,
	RespTimeout:                     10 * time.Second,
	MaximumGossipClockDisparity:     500 * time.Millisecond,
	MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
	MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
	Eth2key:                         "eth2",
	AttSubnetKey:                    "attnets",
	SyncCommsSubnetKey:              "syncnets",
	MinimumPeersInSubnetSearch:      20,
	ContractDeploymentBlock:         4367322,
	bootNodes:                       BootstrapNodes[Goerli],
}

func NetworkConfigFromNetworkType(networkType NetworkType) *NetworkConfig {
	switch networkType {
	case Mainnet:
		return mainnetNetworkConfig
	case Sepolia:
		return sepoliaNetworkConfig
	case Goerli:
		return goerliNetworkConfig
	default:
		return nil
	}
}

func (nc *NetworkConfig) BootNodes() []string {
	return nc.bootNodes
}
