package freezer

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const RootPathDataFile = "data.bin"
const RootPathSidecarFile = "sidecar.bin"

type RootPathOsFs struct {
	Root string
}

func (f *RootPathOsFs) resolveFileName(namespace string, object string, id string, extra ...string) (string, error) {
	root := filepath.Clean(f.Root)
	j := filepath.Join(root, namespace, object, id)
	if !strings.HasPrefix(j, root) {
		return "", os.ErrInvalid
	}
	return j, nil
}

func (f *RootPathOsFs) Get(namespace string, object string, id string, extra ...string) (data io.ReadCloser, sidecar []byte, err error) {
	infoPath, err := f.resolveFileName(namespace, object, id)
	if err != nil {
		return nil, nil, err
	}
	fp, err := os.Open(path.Join(infoPath, RootPathDataFile))
	if err != nil {
		return nil, nil, err
	}
	blob, err := os.ReadFile(path.Join(infoPath, RootPathSidecarFile))
	if err == nil {
		sidecar = blob
	}
	return fp, sidecar, nil
}

func (f *RootPathOsFs) Put(data io.Reader, sidecar []byte, namespace string, object string, id string, extra ...string) error {
	infoPath, err := f.resolveFileName(namespace, object, id)
	if err != nil {
		return err
	}
	_ = os.MkdirAll(infoPath, 0o755)
	fp, err := os.OpenFile(path.Join(infoPath, RootPathDataFile), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = io.Copy(fp, data)
	if err != nil {
		return err
	}
	if sidecar != nil {
		err = os.WriteFile(path.Join(infoPath, RootPathSidecarFile), sidecar, 0o600)
		if err == nil {
			return err
		}
	}
	return nil
}
