package freezer_test

import (
	"os"
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
	err = b.Put(nil, "../../../test", "a", "b")
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

func TestMemoryStore(t *testing.T) {
	runBlobStoreTest(t, freezer.NewBlobStore(&freezer.InMemory{}))
}

func TestRootPathStore(t *testing.T) {
	fz := &freezer.RootPathOsFs{"test_output"}
	defer os.RemoveAll("test_output")
	runBlobStoreTest(t, freezer.NewBlobStore(fz))
}
