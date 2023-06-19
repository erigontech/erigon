package freezer_test

import (
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/stretchr/testify/assert"
)

func runBlobStoreTest(t *testing.T, b *freezer.BlobStore) {
	var err error
	// put bad item into obj
	err = b.Put(nil, "../../../test", "a", "b")
	assert.ErrorIs(t, err, os.ErrInvalid)
	// get bad item
	_, err = b.Get("../../../test", "a", "b")
	assert.ErrorIs(t, err, os.ErrInvalid)
	// put item into obj
	orig := []byte{1, 2, 3, 4}
	err = b.Put(orig, "test", "a", "b")
	assert.NoError(t, err)

	// get item from obj
	ans, err := b.Get("test", "a", "b")
	assert.NoError(t, err)
	assert.EqualValues(t, orig, ans)

	ans, err = b.Get("test", "b", "a")
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, ans)
}

func runSidecarBlobStoreTest(t *testing.T, b *freezer.SidecarBlobStore) {
	var err error
	// put bad item into obj
	err = b.Put(nil, nil, "../../../test", "a", "b")
	assert.ErrorIs(t, err, os.ErrInvalid)
	// get bad item
	_, _, err = b.Get("../../../test", "a", "b")
	assert.ErrorIs(t, err, os.ErrInvalid)

	// put item into obj
	orig := []byte{1, 2, 3, 4}
	orig2 := []byte{5, 6, 7, 8}
	err = b.Put(orig, orig2, "test", "a", "b")
	assert.NoError(t, err)

	// get item from obj
	ans, sidecar, err := b.Get("test", "a", "b")
	assert.NoError(t, err)
	assert.EqualValues(t, orig, ans)
	assert.EqualValues(t, orig2, sidecar)

	ans, sidecar, err = b.Get("test", "b", "a")
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, ans)
	assert.Nil(t, sidecar)

	// put item without sidecar
	err = b.Put(orig2, nil, "test", "a", "c")
	assert.NoError(t, err)

	// get item from obj
	ans, sidecar, err = b.Get("test", "a", "c")
	assert.NoError(t, err)
	assert.EqualValues(t, orig2, ans)
	assert.Nil(t, sidecar)
}

func testFreezer(t *testing.T, fn func() (freezer.Freezer, func())) {
	t.Run("BlobStore", func(t *testing.T) {
		f, cn := fn()
		defer cn()
		runBlobStoreTest(t, freezer.NewBlobStore(f))
	})
	t.Run("SidecarBlobStore", func(t *testing.T) {
		f, cn := fn()
		defer cn()
		runSidecarBlobStoreTest(t, freezer.NewSidecarBlobStore(f))
	})
}

func TestMemoryStore(t *testing.T) {
	testFreezer(t, func() (freezer.Freezer, func()) {
		return &freezer.InMemory{}, func() {}
	})
}

func TestRootPathStore(t *testing.T) {
	cnt := atomic.Uint64{}
	c := func() (freezer.Freezer, func()) {
		base := path.Join("test_output", strconv.Itoa(int(cnt.Load())))
		return &freezer.RootPathOsFs{base}, func() { os.RemoveAll(base) }
	}
	testFreezer(t, c)
}
