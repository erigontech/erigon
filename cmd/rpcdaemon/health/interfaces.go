package health

import (
	"context"
)

type NetAPI interface {
	NetPeerCount(_ context.Context) (uint64, error)
}
