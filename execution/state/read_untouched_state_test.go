package state

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

type failingStorageReader struct {
	storageReader
	err error
}

func (r *failingStorageReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	v, ok, _ := r.storageReader.ReadAccountStorage(addr, key)
	return v, ok, r.err
}

// readUntouchedState must return and record exactly what the full read path does.
func TestReadUntouchedStateMatchesFullPath(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xab})
	present := accounts.InternKey([32]byte{0x01})
	absent := accounts.InternKey([32]byte{0x02})
	seeded := accounts.InternKey([32]byte{0x03})
	newReader := func(err error) *failingStorageReader {
		return &failingStorageReader{
			storageReader: storageReader{addr: addr, storage: map[accounts.StorageKey]uint256.Int{present: *uint256.NewInt(7)}},
			err:           err,
		}
	}
	type result struct {
		val     uint256.Int
		src     ReadSource
		ver     Version
		clean   bool
		err     error
		read    VersionedRead[uint256.Int]
		readOK  bool
		origin  bool
		readErr error
	}
	run := func(read func(*IntraBlockState, accounts.Address, accounts.StorageKey) (uint256.Int, ReadSource, Version, bool, error), key accounts.StorageKey, readErr error) result {
		vm := NewVersionMap(nil)
		vm.StoreOriginStorage(addr, seeded, *uint256.NewInt(9))
		ibs := NewWithVersionMap(newReader(readErr), vm)
		defer ibs.Close()
		ibs.SetTxContext(1, 3)
		require.True(t, ibs.untouchedInBlock(addr))
		var r result
		r.val, r.src, r.ver, r.clean, r.err = read(ibs, addr, key)
		r.read, r.readOK = ibs.reads.GetStorage(addr, key)
		_, r.origin = ibs.origins.GetStorage(addr, key)
		r.readErr = ibs.stateReadErr
		return r
	}
	readErr := errors.New("read failed")
	for name, tc := range map[string]struct {
		key accounts.StorageKey
		err error
	}{
		"present": {present, nil},
		"absent":  {absent, nil},
		"seeded":  {seeded, nil},
		"failed":  {present, readErr},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, run(readStateCore, tc.key, tc.err), run(readUntouchedState, tc.key, tc.err))
		})
	}
}
