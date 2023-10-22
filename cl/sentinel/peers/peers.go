package peers

const (
	maxBadPeers       = 50000
	maxPeerRecordSize = 1000
	DefaultMaxPeers   = 16
	MaxBadResponses   = 50
)

type PeeredObject[T any] struct {
	Peer string
	Data T
}
