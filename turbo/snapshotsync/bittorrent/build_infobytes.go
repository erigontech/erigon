package bittorrent

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/anacrolix/torrent/metainfo"
)

func BuildInfoBytesForLMDBSnapshot(root string) (metainfo.Info, error) {
	path := root + "/" + LmdbFilename
	fi, err := os.Stat(path)
	if err != nil {
		return metainfo.Info{}, err
	}
	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return metainfo.Info{}, fmt.Errorf("error getting relative path: %s", err)
	}

	info := metainfo.Info{
		Name:        filepath.Base(root),
		PieceLength: DefaultChunkSize,
		Length:      fi.Size(),
		Files: []metainfo.FileInfo{
			{
				Length:   fi.Size(),
				Path:     []string{relPath},
				PathUTF8: nil,
			},
		},
	}

	err = info.GeneratePieces(func(fi metainfo.FileInfo) (io.ReadCloser, error) {
		return os.Open(filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
	})
	if err != nil {
		err = fmt.Errorf("error generating pieces: %s", err)
		return metainfo.Info{}, err
	}
	return info, nil
}
