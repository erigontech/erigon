package borfinality

import (
	"fmt"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

// chainValidatorFake is a mock for the chain validator service
type chainValidatorFake struct {
	validate func(currentHeader *types.Header, chain []*types.Header) (bool, error)
}

// chainReaderFake is a mock for the chain reader service
type chainReaderFake struct {
	getTd func(hash libcommon.Hash, number uint64) *big.Int
}

func newChainValidatorFake(validate func(currentHeader *types.Header, chain []*types.Header) (bool, error)) *chainValidatorFake {
	return &chainValidatorFake{validate: validate}
}

func newChainReaderFake(getTd func(hash libcommon.Hash, number uint64) *big.Int) *chainReaderFake {
	return &chainReaderFake{getTd: getTd}
}

func TestPastChainInsert(t *testing.T) {
	m := stages.Mock(t)

	// tx, _ := m.DB.BeginRo(context.Background())
	// defer tx.Rollback()

	chainA, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 70 /* n */, func(i int, b *core.BlockGen) {
	}, false /* intermediateHashes */)
	require.NoError(t, err)

	err = m.InsertChain(chainA)
	require.NoError(t, err)

	chainB, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 70 /* n */, func(i int, b *core.BlockGen) {
		if i > 64 {
			b.OffsetTime(-1)
		}
	}, false /* intermediateHashes */)
	require.NoError(t, err)

	err = m.InsertChain(chainB.Slice(67, 69))
	require.NoError(t, err)

	// chainC, err := core.GenerateChain(m.ChainConfig, chainA.Blocks[53], m.Engine, m.DB, 10 /* n */, func(i int, b *core.BlockGen) {
	// }, false /* intermediateHashes */)
	// require.NoError(t, err)

	// err = m.InsertChain(chainC)
	// require.NoError(t, err)

	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	h := rawdb.ReadHeaderByNumber(tx, uint64(64))
	tx.Rollback()

	print("chainA", chainA.Headers)
	print("chainB", chainB.Headers)
	// print("chainC", chainC.Headers)

	fmt.Printf("head - %+v - hash: %v\n", h.Number.Uint64(), h.Hash())

	// p := rawdb.ReadHeader(tx, chainB.TopBlock.Hash(), chainB.TopBlock.NumberU64())

	// require.Equal(t, chainC.Headers[9].Hash(), h)
	// require.Nil(t, p)
}

func print(prefix string, headers []*types.Header) {
	fmt.Println("---"+prefix, ", length:", len(headers))
	fmt.Printf("headers[0]: %d - %s\n", headers[0].Number.Uint64(), headers[0].Hash())
	fmt.Printf("headers[n-1]: %d - %s\n", headers[len(headers)-1].Number.Uint64(), headers[len(headers)-1].Hash())
}

// func TestFutureChainInsert(t *testing.T) {
// 	t.Parallel()

// 	var (
// 		db      = memdb.NewTestDB(t)
// 		genesis = (&core.Genesis{BaseFee: big.NewInt(params.InitialBaseFee)}).MustCommit(db, "")
// 	)

// 	hc, err := NewHeaderChain(db, params.AllEthashProtocolChanges, ethash.NewFaker(), func() bool { return false })
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// Create mocks for forker
// 	getTd := func(hash libcommon.Hash, number uint64) *big.Int {
// 		return big.NewInt(int64(number))
// 	}
// 	validate := func(currentHeader *types.Header, chain []*types.Header) (bool, error) {
// 		// Put all explicit conditions here
// 		// If canonical chain is empty and we're importing a chain of 64 blocks
// 		if currentHeader.Number.Uint64() == uint64(0) && len(chain) == 64 {
// 			return true, nil
// 		}
// 		// If length of future chains > some value, they should not be accepted
// 		if currentHeader.Number.Uint64() == uint64(64) && len(chain) <= 10 {
// 			return true, nil
// 		}

// 		return false, nil
// 	}
// 	mockChainReader := newChainReaderFake(getTd)
// 	mockChainValidator := newChainValidatorFake(validate)
// 	mockForker := NewForkChoice(mockChainReader, nil, mockChainValidator)

// 	// chain A: G->A1->A2...A64
// 	chainA := makeHeaderChain(genesis.Header(), 64, ethash.NewFaker(), db, 10)

// 	// Inserting 64 headers on an empty chain
// 	// expecting 1 write status with no error
// 	testInsert(t, hc, chainA, CanonStatTy, nil, mockForker)

// 	// The current chain is: G->A1->A2...A64
// 	// chain B: G->A1->A2...A64->B65->B66...B84
// 	chainB := makeHeaderChain(chainA[63], 20, ethash.NewFaker(), db, 10)

// 	// Inserting 20 headers on the canonical chain
// 	// expecting 0 write status with no error
// 	testInsert(t, hc, chainB, SideStatTy, nil, mockForker)

// 	// The current chain is: G->A1->A2...A64
// 	// chain C: G->A1->A2...A64->C65->C66...C74
// 	chainC := makeHeaderChain(chainA[63], 10, ethash.NewFaker(), db, 10)

// 	// Inserting 10 headers on the canonical chain
// 	// expecting 0 write status with no error
// 	testInsert(t, hc, chainC, CanonStatTy, nil, mockForker)
// }

// func TestOverlappingChainInsert(t *testing.T) {
// 	t.Parallel()

// 	var (
// 		db      = memdb.NewTestDB(t)
// 		genesis = (&core.Genesis{BaseFee: big.NewInt(params.InitialBaseFee)}).MustCommit(db, "")
// 	)

// 	hc, err := NewHeaderChain(db, params.AllEthashProtocolChanges, ethash.NewFaker(), func() bool { return false })
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// Create mocks for forker
// 	getTd := func(hash libcommon.Hash, number uint64) *big.Int {
// 		return big.NewInt(int64(number))
// 	}
// 	validate := func(currentHeader *types.Header, chain []*types.Header) (bool, error) {
// 		// Put all explicit conditions here
// 		// If canonical chain is empty and we're importing a chain of 64 blocks
// 		if currentHeader.Number.Uint64() == uint64(0) && len(chain) == 64 {
// 			return true, nil
// 		}
// 		// If length of chain is > some fixed value then don't accept it
// 		if currentHeader.Number.Uint64() == uint64(64) && len(chain) <= 20 {
// 			return true, nil
// 		}

// 		return false, nil
// 	}
// 	mockChainReader := newChainReaderFake(getTd)
// 	mockChainValidator := newChainValidatorFake(validate)
// 	mockForker := NewForkChoice(mockChainReader, nil, mockChainValidator)

// 	// chain A: G->A1->A2...A64
// 	chainA := makeHeaderChain(genesis.Header(), 64, ethash.NewFaker(), db, 10)

// 	// Inserting 64 headers on an empty chain
// 	// expecting 1 write status with no error
// 	testInsert(t, hc, chainA, CanonStatTy, nil, mockForker)

// 	// The current chain is: G->A1->A2...A64
// 	// chain B: G->A1->A2...A54->B55->B56...B84
// 	chainB := makeHeaderChain(chainA[53], 30, ethash.NewFaker(), db, 10)

// 	// Inserting 20 blocks on canonical chain
// 	// expecting 2 write status with no error
// 	testInsert(t, hc, chainB, SideStatTy, nil, mockForker)

// 	// The current chain is: G->A1->A2...A64
// 	// chain C: G->A1->A2...A54->C55->C56...C74
// 	chainC := makeHeaderChain(chainA[53], 20, ethash.NewFaker(), db, 10)

// 	// Inserting 10 blocks on canonical chain
// 	// expecting 1 write status with no error
// 	testInsert(t, hc, chainC, CanonStatTy, nil, mockForker)
// }

// Mock chain reader functions
//
//	func (c *chainReaderFake) Config() *params.ChainConfig {
//		return &params.ChainConfig{TerminalTotalDifficulty: nil}
//	}
func (c *chainReaderFake) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	return c.getTd(hash, number)
}

// Mock chain validator functions
func (w *chainValidatorFake) IsValidPeer(fetchHeadersByNumber func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []libcommon.Hash, error)) (bool, error) {
	return true, nil
}
func (w *chainValidatorFake) IsValidChain(current *types.Header, headers []*types.Header) (bool, error) {
	return w.validate(current, headers)
}
func (w *chainValidatorFake) ProcessCheckpoint(endBlockNum uint64, endBlockHash libcommon.Hash) {}
func (w *chainValidatorFake) ProcessMilestone(endBlockNum uint64, endBlockHash libcommon.Hash)  {}
func (w *chainValidatorFake) ProcessFutureMilestone(num uint64, hash libcommon.Hash) {
}
func (w *chainValidatorFake) GetWhitelistedCheckpoint() (bool, uint64, libcommon.Hash) {
	return false, 0, libcommon.Hash{}
}

func (w *chainValidatorFake) GetWhitelistedMilestone() (bool, uint64, libcommon.Hash) {
	return false, 0, libcommon.Hash{}
}
func (w *chainValidatorFake) PurgeWhitelistedCheckpoint() {}
func (w *chainValidatorFake) PurgeWhitelistedMilestone()  {}
func (w *chainValidatorFake) GetCheckpoints(current, sidechainHeader *types.Header, sidechainCheckpoints []*types.Header) (map[uint64]*types.Header, error) {
	return map[uint64]*types.Header{}, nil
}
func (w *chainValidatorFake) LockMutex(endBlockNum uint64) bool {
	return false
}
func (w *chainValidatorFake) UnlockMutex(doLock bool, milestoneId string, endBlockHash libcommon.Hash) {
}
func (w *chainValidatorFake) UnlockSprint(endBlockNum uint64) {
}
func (w *chainValidatorFake) RemoveMilestoneID(milestoneId string) {
}
func (w *chainValidatorFake) GetMilestoneIDsList() []string {
	return nil
}
