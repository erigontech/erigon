package snapshotsync

import (
	"errors"

	"github.com/ledgerwatch/erigon/turbo/snapshotsync/trackers"
)

const (
	// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
	// amount of network announcements, but can't go over 2Mb
	// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
	DefaultPieceSize = 2 * 1024 * 1024
	MdbxFilename     = "mdbx.dat"
	EpochSize        = 500_000
)

var (
	ErrInvalidSnapshot = errors.New("this snapshot for this chainID not supported ")
)

// Trackers - break down by priority tier
var Trackers = [][]string{
	trackers.Best, trackers.Ws, //trackers.Udp, trackers.Https, trackers.Http,
}
