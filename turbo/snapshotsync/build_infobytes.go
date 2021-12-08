package snapshotsync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

func BuildAllTorrentInfo(root string) ([]metainfo.Info, error) {
	files, err := segments(root, Headers)
	if err != nil {
		return nil, err
	}
	var res []metainfo.Info
	for _, f := range files {
		info, err := BuildInfoBytesForSnapshot(root, f)
		if err != nil {
			return nil, err
		}
		res = append(res, info)
	}
	return res, nil
}

func BuildInfoBytesForSnapshot(root string, fileName string) (metainfo.Info, error) {
	path := filepath.Join(root, fileName)
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
		PieceLength: DefaultPieceSize,
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

func BuildInfoDir(root string) (metainfo.MetaInfo, metainfo.Info, error) {
	mi := metainfo.MetaInfo{
		//AnnounceList: builtinAnnounceList,
	}
	//for _, a := range args.AnnounceList {
	//	mi.AnnounceList = append(mi.AnnounceList, []string{a})
	//}
	mi.SetDefaults()
	//	mi.Comment = args.Comment
	//	mi.CreatedBy = args.CreatedBy
	info := metainfo.Info{
		PieceLength: 256 * 1024,
	}
	err := info.BuildFromFilePath(root)
	if err != nil {
		return metainfo.MetaInfo{}, metainfo.Info{}, err
	}
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		return metainfo.MetaInfo{}, metainfo.Info{}, err
	}
	return mi, info, nil
}
