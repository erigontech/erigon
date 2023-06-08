package freezer

import "io"

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
