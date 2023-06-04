package freezer

import (
	"bytes"
	"io"
)

type Freezer interface {
	Getter
	Putter
}

type Getter interface {
	Get(namespace, object, id string, extra ...string) (data io.ReadCloser, sidecar []byte, err error)
}

type Putter interface {
	Put(data io.Reader, sidecar []byte, namespace, object, id string, extra ...string) error
}

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
