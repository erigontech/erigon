package downloader

import (
	"context"
	"fmt"
	"iter"
	"path/filepath"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// Localizes paths to the provided directory for translation on the receiver side on a different
// host.
type RpcClient struct {
	inner   downloaderproto.DownloaderClient
	rootDir string
}

func (me *RpcClient) fixPath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return path, nil
	}
	rel, err := filepath.Rel(me.rootDir, path)
	if err != nil {
		return "", errRpcSnapName{fmt.Errorf("failed to get relative path from %q to %q: %w", path, me.rootDir, err)}
	}
	if !filepath.IsLocal(rel) {
		return "", errRpcSnapName{fmt.Errorf("relative path %q is not local to %q", rel, me.rootDir)}
	}
	return rel, nil
}

func (me *RpcClient) fixPaths(paths iter.Seq[*string]) (err error) {
	for p := range paths {
		*p, err = me.fixPath(*p)
		if err != nil {
			return
		}
	}
	return
}

// Iterates over elements of a slice yielding pointers to the values so they can be modified.
func mutSlice[T any](sl []T) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		for i := range sl {
			if !yield(&sl[i]) {
				return
			}
		}
	}
}

func (me *RpcClient) Seed(ctx context.Context, paths []string) (err error) {
	err = me.fixPaths(mutSlice(paths))
	if err != nil {
		return
	}
	_, err = me.inner.Seed(ctx, &downloaderproto.SeedRequest{Paths: paths})
	return
}

func (me *RpcClient) Download(ctx context.Context, request *downloaderproto.DownloadRequest) (err error) {
	err = me.fixPaths(func(yield func(*string) bool) {
		for i := range request.Items {
			if !yield(&request.Items[i].Path) {
				return
			}
		}
	})
	if err != nil {
		return
	}
	_, err = me.inner.Download(ctx, request)
	return
}

func (me *RpcClient) Delete(ctx context.Context, paths []string) (err error) {
	err = me.fixPaths(mutSlice(paths))
	if err != nil {
		return
	}
	_, err = me.inner.Delete(ctx, &downloaderproto.DeleteRequest{Paths: paths})
	return
}

// Completed reports snapshot-download progress in bytes. ok is false when the
// underlying client can't report it (e.g. an external downloader over gRPC).
func (me *RpcClient) Completed(ctx context.Context) (done, total uint64, ok bool, err error) {
	c, canReport := me.inner.(progressReporter)
	if !canReport {
		return 0, 0, false, nil
	}
	done, total, ok = c.Completed()
	return done, total, ok, nil
}

// ResetProgress drops any stale progress sample; no-op for a remote client.
func (me *RpcClient) ResetProgress() {
	if c, ok := me.inner.(progressReporter); ok {
		c.ResetStats()
	}
}

func NewRpcClient(inner downloaderproto.DownloaderClient, rootDir string) *RpcClient {
	return &RpcClient{inner: inner, rootDir: rootDir}
}

var (
	_ dbservices.DownloaderClient       = (*RpcClient)(nil)
	_ dbservices.DownloadProgressReport = (*RpcClient)(nil)
)
