package cltypes

type MetadataV1 struct {
	SeqNumber uint64
	Attnets   uint64
}

type MetadataV2 struct {
	SeqNumber uint64
	Attnets   uint64
	Syncnets  uint64
}

// ENRForkID contains current fork id for ENR entries.
type ENRForkID struct {
	CurrentForkDigest [4]byte `ssz-size:"4" `
	NextForkVersion   [4]byte `ssz-size:"4" `
	NextForkEpoch     uint64
}

// ForkData contains current fork id for gossip subscription.
type ForkData struct {
	CurrentVersion        [4]byte  `ssz-size:"4" `
	GenesisValidatorsRoot [32]byte `ssz-size:"32" `
}

// Ping is a test P2P message, used to test out liveness of our peer/signaling disconnection.
type Ping struct {
	Id uint64 `json:"id" `
}

// P2P Message for bootstrap
type SingleRoot struct {
	Root [32]byte `ssz-size:"32" `
}

/*
 * LightClientUpdatesByRangeRequest that helps syncing to chain tip from a past point.
 * It takes the Period of the starting update and the amount of updates we want (MAX: 128).
 */
type LightClientUpdatesByRangeRequest struct {
	Period uint64
	Count  uint64
}

/*
 * Status is a P2P Message we exchange when connecting to a new Peer.
 * It contains network information about the other peer and if mismatching we drop it.
 */
type Status struct {
	ForkDigest     [4]byte  `ssz-size:"4"`
	FinalizedRoot  [32]byte `ssz-size:"32"`
	FinalizedEpoch uint64
	HeadRoot       [32]byte `ssz-size:"32"`
	HeadSlot       uint64
}

/*
 * SigningData is the message we want to verify against the sync committee signature.
 * Root is the HastTreeRoot() of the beacon block header,
 * while the domain is the sync committee identifier.
 */
type SigningData struct {
	Root   [32]byte `ssz-size:"32"`
	Domain []byte   `ssz-size:"32"`
}
