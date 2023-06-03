package freezer

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type RootPathOsFs struct {
	Root string
}

func (f *RootPathOsFs) resolveFileName(namespace string, object string, id string, extra ...string) (string, error) {
	root := filepath.Clean(f.Root)
	j := filepath.Join(root, namespace, object, id)
	if !strings.HasPrefix(j, root) {
		return "", &fs.PathError{
			Op:   "lookup",
			Path: j,
			Err:  fmt.Errorf("path not in root"),
		}
	}
	return j, nil
}

func (f *RootPathOsFs) Get(namespace string, object string, id string, extra ...string) (data io.ReadCloser, sidecar []byte, err error) {
	infoPath, err := f.resolveFileName(namespace, object, id)
	if err != nil {
		return nil, nil, err
	}
	fp, err := os.Open(path.Join(infoPath, "data.bin"))
	if err != nil {
		return nil, nil, err
	}
	blob, err := os.ReadFile(path.Join(infoPath, "sidecar.bin"))
	if err == nil {
		sidecar = blob
	}
	return fp, blob, nil
}

func (f *RootPathOsFs) Put(data io.Reader, sidecar []byte, namespace string, object string, id string, extra ...string) error {
	infoPath, err := f.resolveFileName(namespace, object, id)
	if err != nil {
		return err
	}
	_ = os.MkdirAll(infoPath, 0o755)
	fp, err := os.OpenFile(path.Join(infoPath, "data.bin"), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = io.Copy(fp, data)
	if err != nil {
		return err
	}
	err = os.WriteFile(path.Join(infoPath, "sidecar.bin"), sidecar, 0o755)
	if err == nil {
		return err
	}
	return nil
}
