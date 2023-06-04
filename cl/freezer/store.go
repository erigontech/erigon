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
	defer o.Close()
	return io.ReadAll(o)
}

func (b *BlobStore) Put(dat []byte, namespace, object, id string) error {
	return b.f.Put(bytes.NewBuffer(dat), nil, namespace, object, id)
}

type SidecarBlobStore struct {
	f Freezer
}

func NewSidecarBlobStore(f Freezer) *SidecarBlobStore {
	return &SidecarBlobStore{f: f}
}

func (b *SidecarBlobStore) Get(namespace, object, id string) (blob []byte, sidecar []byte, err error) {
	a, bb, err := b.f.Get(namespace, object, id)
	if err != nil {
		return nil, nil, err
	}
	defer a.Close()
	sidecar = bb
	blob, err = io.ReadAll(a)
	if err != nil {
		return
	}
	return
}

func (b *SidecarBlobStore) Put(dat []byte, sidecar []byte, namespace, object, id string) error {
	return b.f.Put(bytes.NewBuffer(dat), sidecar, namespace, object, id)
}
