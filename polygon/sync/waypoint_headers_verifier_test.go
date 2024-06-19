package sync

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

func TestVerifyCheckpointHeaders(t *testing.T) {
	header := &types.Header{
		TxHash: common.HexToHash("0x01"),
		Number: big.NewInt(42),
	}
	rootHash, err := bor.ComputeHeadersRootHash([]*types.Header{header})
	require.NoError(t, err)
	checkpoint := &heimdall.Milestone{
		Fields: heimdall.WaypointFields{
			RootHash: common.BytesToHash(rootHash),
		},
	}

	err = VerifyCheckpointHeaders(checkpoint, []*types.Header{header})
	require.NoError(t, err)

	diffHeader := &types.Header{
		TxHash: common.HexToHash("0x02"),
		Number: big.NewInt(42),
	}
	err = VerifyCheckpointHeaders(checkpoint, []*types.Header{diffHeader})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)

	err = VerifyCheckpointHeaders(checkpoint, []*types.Header{})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)
}

func TestVerifyMilestoneHeaders(t *testing.T) {
	header := &types.Header{
		Root: common.HexToHash("0x01"),
	}
	milestone := &heimdall.Milestone{
		Fields: heimdall.WaypointFields{
			RootHash: header.Hash(),
		},
	}

	err := VerifyMilestoneHeaders(milestone, []*types.Header{header})
	require.NoError(t, err)

	diffHeader := &types.Header{
		Root: common.HexToHash("0x02"),
	}
	err = VerifyMilestoneHeaders(milestone, []*types.Header{diffHeader})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)

	err = VerifyMilestoneHeaders(milestone, []*types.Header{})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)
}
