package misc

import (
	"crypto/sha256"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/assert"
)

type dummyChainHeaderReader struct {
}

func (r dummyChainHeaderReader) Config() *chain.Config {
	return nil
}

func (r dummyChainHeaderReader) CurrentHeader() *types.Header {
	return nil
}

func (r dummyChainHeaderReader) GetHeader(libcommon.Hash, uint64) *types.Header {
	return nil
}

func (r dummyChainHeaderReader) GetHeaderByNumber(uint64) *types.Header {
	return nil
}

func (r dummyChainHeaderReader) GetHeaderByHash(libcommon.Hash) *types.Header {
	return nil
}

func (r dummyChainHeaderReader) GetTd(libcommon.Hash, uint64) *big.Int {
	return nil
}

func (r dummyChainHeaderReader) FrozenBlocks() uint64 {
	return 0
}

type dummyStateReader struct {
}

func (dsr *dummyStateReader) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	return nil, nil
}
func (dsr *dummyStateReader) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	return nil, nil
}
func (dsr *dummyStateReader) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	return make([]byte, 0), nil
}
func (dsr *dummyStateReader) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	return 0, nil
}
func (dsr *dummyStateReader) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}

func TestApplyBeaconRoot(t *testing.T) {
	var mockReader dummyChainHeaderReader
	testHashBytes := sha256.Sum256([]byte("test"))
	testRootHash := libcommon.BytesToHash(testHashBytes[:])
	header := types.Header{
		ParentBeaconBlockRoot: &testRootHash,
		Time:                  1,
	}

	var state state.IntraBlockState = *state.New(&dummyStateReader{})

	ApplyBeaconRootEip4788(mockReader, &header, &state)
	pc := vm.PrecompiledContractsCancun[libcommon.BytesToAddress(params.HistoryStorageAddress)]
	spc, ok := pc.(vm.StatefulPrecompiledContract)
	if !ok {
		t.Fatalf("Error instantiating pre-compile")
	}
	timestampParam := uint256.NewInt(1).Bytes32()

	res, err := spc.RunStateful(timestampParam[:], &state)
	if err != nil {
		t.Errorf("error %v", err)
	}
	assert.Equal(t, testRootHash.Bytes(), res, "Beacon root mismatch")
	t.Logf("result %v", res)
}
