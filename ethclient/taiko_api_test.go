package ethclient

import (
	"context"
	"crypto/rand"
	"math/big"
	"testing"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/stretchr/testify/require"
)

func TestHeadL1Origin(t *testing.T) {
	backend, chain, err := newTestBackend(t)
	require.NoError(t, err)
	defer backend.Stop()
	client := backend.Attach()
	ec := NewClient(client)
	defer ec.Close()
	headerHash := chain.Blocks[len(chain.Blocks)-1].Hash()

	l1OriginFound, err := ec.HeadL1Origin(context.Background())

	require.Equal(t, ethereum.NotFound.Error(), err.Error())
	require.Nil(t, l1OriginFound)

	testL1Origin := &rawdb.L1Origin{
		BlockID:       randomBigInt(),
		L2BlockHash:   headerHash,
		L1BlockHeight: randomBigInt(),
		L1BlockHash:   randomHash(),
	}
	tx, err := backend.ChainDB().BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	err = rawdb.WriteL1Origin(tx, testL1Origin.BlockID, testL1Origin)
	require.NoError(t, err)
	err = rawdb.WriteHeadL1Origin(tx, testL1Origin.BlockID)
	require.NoError(t, err)
	tx.Commit()
	l1OriginFound, err = ec.HeadL1Origin(context.Background())

	require.Nil(t, err)
	require.Equal(t, testL1Origin, l1OriginFound)
}

func TestL1OriginByID(t *testing.T) {
	backend, chain, err := newTestBackend(t)
	require.NoError(t, err)
	defer backend.Stop()
	client := backend.Attach()
	ec := NewClient(client)
	defer ec.Close()

	headerHash := chain.Blocks[len(chain.Blocks)-1].Hash()
	testL1Origin := &rawdb.L1Origin{
		BlockID:       randomBigInt(),
		L2BlockHash:   headerHash,
		L1BlockHeight: randomBigInt(),
		L1BlockHash:   randomHash(),
	}

	l1OriginFound, err := ec.L1OriginByID(context.Background(), testL1Origin.BlockID)
	require.Equal(t, ethereum.NotFound.Error(), err.Error())
	require.Nil(t, l1OriginFound)

	tx, err := backend.ChainDB().BeginRw(context.Background())
	require.NoError(t, err)
	err = rawdb.WriteL1Origin(tx, testL1Origin.BlockID, testL1Origin)
	require.NoError(t, err)
	err = rawdb.WriteHeadL1Origin(tx, testL1Origin.BlockID)
	require.NoError(t, err)
	tx.Commit()

	l1OriginFound, err = ec.L1OriginByID(context.Background(), testL1Origin.BlockID)

	require.Nil(t, err)
	require.Equal(t, testL1Origin, l1OriginFound)
}

// randomHash generates a random blob of data and returns it as a hash.
func randomHash() common.Hash {
	var hash common.Hash
	if n, err := rand.Read(hash[:]); n != length.Hash || err != nil {
		panic(err)
	}
	return hash
}

// randomBigInt generates a random big integer.
func randomBigInt() *big.Int {
	randomBigInt, err := rand.Int(rand.Reader, common.Big256)
	if err != nil {
		log.Crit(err.Error())
	}

	return randomBigInt
}
