package freezer

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type InMemory struct {
	blob sync.Map

	mu sync.RWMutex
}

func (f *InMemory) save(name string, b io.Reader) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(b)
	f.blob.Store(name, buf)
}

func (f *InMemory) get(name string) (*bytes.Buffer, error) {
	val, ok := f.blob.Load(name)
	if !ok {
		return nil, fs.ErrNotExist
	}
	cast, ok := val.(*bytes.Buffer)
	if !ok {
		panic("incorrect item in sync map... this should never happen")
	}
	return cast, nil
}

func (f *InMemory) resolveFileName(namespace string, object string, id string, extra ...string) (string, error) {
	j := filepath.Join("inmem", namespace, object, id)
	if !strings.HasPrefix(j, "inmem") {
		return "", os.ErrInvalid
	}
	return j, nil
}

func (f *InMemory) Get(namespace string, object string, id string, extra ...string) (data io.ReadCloser, sidecar []byte, err error) {
	infoPath, err := f.resolveFileName(namespace, object, id)
	if err != nil {
		return nil, nil, err
	}
	fp, err := f.get(path.Join(infoPath, RootPathDataFile))
	if err != nil {
		return nil, nil, err
	}
	blob, err := f.get(path.Join(infoPath, RootPathSidecarFile))
	if err == nil {
		sidecar = blob.Bytes()
	}
	return io.NopCloser(fp), sidecar, nil
}

func (f *InMemory) Put(data io.Reader, sidecar []byte, namespace string, object string, id string, extra ...string) error {
	infoPath, err := f.resolveFileName(namespace, object, id)
	if err != nil {
		return err
	}
	f.save(path.Join(infoPath, RootPathDataFile), data)
	if sidecar != nil {
		f.save(path.Join(infoPath, RootPathSidecarFile), bytes.NewBuffer(sidecar))
	}
	return nil
}
