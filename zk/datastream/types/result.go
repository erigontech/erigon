package types

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type CommandError uint32

const (
	ResultEntryMinSize = uint32(9)

	// Command errors
	CmdErrOK              = 0 // CmdErrOK for no error
	CmdErrAlreadyStarted  = 1 // CmdErrAlreadyStarted for client already started error
	CmdErrAlreadyStopped  = 2 // CmdErrAlreadyStopped for client already stopped error
	CmdErrBadFromEntry    = 3 // CmdErrBadFromEntry for invalid starting entry number
	CmdErrBadFromBookmark = 4 // CmdErrBadFromBookmark for invalid starting bookmark
	CmdErrInvalidCommand  = 9 // CmdErrInvalidCommand for invalid/unknown command error
)

type ResultEntry struct {
	PacketType uint8 // 0xff:Result
	Length     uint32
	ErrorNum   uint32 // 0:No error
	ErrorStr   []byte
}

// checks is the result error number is OK
func (r ResultEntry) IsOk() bool {
	return r.ErrorNum == CmdErrOK
}

// returns the error string if the result error number is not OK
// returns nil otherwise
func (r *ResultEntry) GetError() error {
	if r.IsOk() {
		return nil
	}

	return errors.New(string(r.ErrorStr))
}

// Encode encodes result entry to the binary format
func (r *ResultEntry) Encode() []byte {
	be := make([]byte, 1)
	be[0] = r.PacketType
	be = binary.BigEndian.AppendUint32(be, r.Length)
	be = binary.BigEndian.AppendUint32(be, r.ErrorNum)
	be = append(be, r.ErrorStr...) //nolint:makezero
	return be
}

// Decode/convert from binary bytes slice to an entry type
func DecodeResultEntry(b []byte) (*ResultEntry, error) {
	if uint32(len(b)) < ResultEntryMinSize {
		return &ResultEntry{}, fmt.Errorf("invalid result entry binary size. Expected: >=%d, got: %d", ResultEntryMinSize, len(b))
	}

	length := binary.BigEndian.Uint32(b[1:5])
	if length < ResultEntryMinSize {
		return &ResultEntry{}, fmt.Errorf("invalid result.length value. Expected: >=%d, got: %d", ResultEntryMinSize, length)
	}

	errorStr := b[9:]

	if uint32(len(errorStr)) != length-ResultEntryMinSize {
		return &ResultEntry{}, fmt.Errorf("invalid result entry error binary size. Expected: %d, got: %d", length-ResultEntryMinSize, len(errorStr))
	}

	return &ResultEntry{
		PacketType: b[0],
		Length:     length,
		ErrorNum:   binary.BigEndian.Uint32(b[5:9]),
		ErrorStr:   errorStr,
	}, nil
}
