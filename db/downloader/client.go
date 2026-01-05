package downloader

import (
	"context"
	"fmt"
	"iter"
	"path/filepath"

	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

type RpcClient struct {
	inner   downloaderproto.DownloaderClient
	rootDir string
}

func (me RpcClient) fixPath(path string) (string, error) {
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

func (me RpcClient) fixPaths(paths iter.Seq[*string]) (err error) {
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

func (g RpcClient) Seed(ctx context.Context, paths []string) (err error) {
	err = g.fixPaths(mutSlice(paths))
	if err != nil {
		return
	}
	_, err = g.inner.Seed(ctx, &downloaderproto.SeedRequest{Paths: paths})
	return
}

func (g RpcClient) Download(ctx context.Context, request *downloaderproto.DownloadRequest) (err error) {
	err = g.fixPaths(func(yield func(*string) bool) {
		for i := range request.Items {
			if !yield(&request.Items[i].Path) {
				return
			}
		}
	})
	if err != nil {
		return
	}
	_, err = g.inner.Download(ctx, request)
	return
}

func (g RpcClient) Delete(ctx context.Context, paths []string) (err error) {
	err = g.fixPaths(mutSlice(paths))
	if err != nil {
		return
	}
	_, err = g.inner.Delete(ctx, &downloaderproto.DeleteRequest{Paths: paths})
	return
}

// Returns a Downloader client for RPC, which means paths localized to the provided directory for
// translation on the receiver side on a different host.
func NewRpcClient(inner downloaderproto.DownloaderClient, rootDir string) RpcClient {
	return RpcClient{inner: inner, rootDir: rootDir}
}

var _ Client = (*RpcClient)(nil)

// Full Client also allowing blocking on downloads. Simplified interface rather than using GRPC directly.
type Client interface {
	SeederClient
	// Request files be downloaded. Returns when the download is complete. Downloader seeds.
	Download(context.Context, *downloaderproto.DownloadRequest) error
}

// Seed and Delete methods, used by pruning and block retiring.
type SeederClient interface {
	// Seed generated file. Downloader will hash.
	Seed(_ context.Context, paths []string) error
	// Remove files from the Downloader.
	Delete(_ context.Context, paths []string) error
}

// A Seeder client that does nothing when delete or seed is requested, a common configuration pattern.
type NoopSeederClient struct{}

func (NoopSeederClient) Seed(_ context.Context, paths []string) error { return nil }

func (NoopSeederClient) Delete(_ context.Context, paths []string) error { return nil }
