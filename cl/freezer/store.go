package freezer

import (
	"bytes"
	"io"
)

type BlobStore struct {
	f Freezer
}

func NewBlobStore(f Freezer) *BlobStore {
	return &BlobStore{f: f}
}

func (b *BlobStore) Get(namespace, object, id string) ([]byte, error) {
	o, _, err := b.f.Get(namespace, object, id)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(o)
}

func (b *BlobStore) Put(dat []byte, namespace, object, id string) error {
	return b.f.Put(bytes.NewBuffer(dat), nil, namespace, object, id)
}
