package snapshotsync

import (
	"errors"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
	"github.com/ledgerwatch/erigon/core/trackers"
	"github.com/ledgerwatch/erigon/params"
)

const (
	// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
	// amount of network announcements, but can't go over 2Mb
	// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
	DefaultPieceSize = 2 * 1024 * 1024
	MdbxFilename     = "mdbx.dat"
	EpochSize        = 500_000

	//todo It'll be changed after enabling new snapshot generation mechanism
	HeadersSnapshotHash = "0000000000000000000000000000000000000000"
	BlocksSnapshotHash  = "0000000000000000000000000000000000000000"
	StateSnapshotHash   = "0000000000000000000000000000000000000000"

	SnapshotInfoHashPrefix  = "ih"
	SnapshotInfoBytesPrefix = "ib"
)

var (
	TorrentHashes = map[uint64]map[snapshotsync.SnapshotType]metainfo.Hash{
		params.MainnetChainConfig.ChainID.Uint64(): {
			snapshotsync.SnapshotType_headers: metainfo.NewHashFromHex(HeadersSnapshotHash),
			snapshotsync.SnapshotType_bodies:  metainfo.NewHashFromHex(BlocksSnapshotHash),
			snapshotsync.SnapshotType_state:   metainfo.NewHashFromHex(StateSnapshotHash),
		},
	}
	ErrInvalidSnapshot = errors.New("this snapshot for this chainID not supported ")
)

func GetAvailableSnapshotTypes(chainID uint64) []snapshotsync.SnapshotType {
	v := TorrentHashes[chainID]
	res := make([]snapshotsync.SnapshotType, 0, len(v))
	for i := range v {
		res = append(res, i)
	}
	return res
}

// Trackers - break down by priority tier
var Trackers = [][]string{
	trackers.Best, trackers.Ws, //trackers.Udp, trackers.Https, trackers.Http,
}
