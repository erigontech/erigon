// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package blob_storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"slices"
	"strings"

	"github.com/golang/snappy"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var errCorruptPack = errors.New("corrupt column pack")

var maxFrame = uint32(binary.MaxVarintLen64 + snappy.MaxEncodedLen(int(clparams.MaxPayloadSize)))

type packFrame struct {
	off uint64
	n   uint32
}

// Pack layout: magic, version, width (u16 LE), the u32 LE frame length of each column (0 means absent) and a
// CRC32C of all these bytes, then the ssz_snappy frames of the present columns in column order.
const (
	packSuffix     = ".pack"
	packMagic      = "CLPK"
	packVersion    = 1
	packPrefixSize = 7
	packCRCSize    = 4
)

func packHeaderSize(width uint64) uint64 { return packPrefixSize + 4*width + packCRCSize }

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendPackHeader(dst []byte, lengths []uint32) []byte {
	start := len(dst)
	dst = append(dst, packMagic...)
	dst = append(dst, packVersion)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(lengths)))
	for _, n := range lengths {
		dst = binary.LittleEndian.AppendUint32(dst, n)
	}
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

func parsePackHeader(r io.ReaderAt, size int64, width uint64) ([]packFrame, error) {
	if size < packPrefixSize {
		return nil, errCorruptPack
	}
	var prefix [packPrefixSize]byte
	if _, err := io.ReadFull(io.NewSectionReader(r, 0, packPrefixSize), prefix[:]); err != nil {
		return nil, err
	}
	if string(prefix[:4]) != packMagic || prefix[4] != packVersion {
		return nil, errCorruptPack
	}
	if width > math.MaxUint16 || uint64(binary.LittleEndian.Uint16(prefix[5:])) != width {
		return nil, errCorruptPack
	}
	h := packHeaderSize(width)
	if h > uint64(size) {
		return nil, errCorruptPack
	}
	header := make([]byte, h)
	if _, err := io.ReadFull(io.NewSectionReader(r, 0, int64(h)), header); err != nil {
		return nil, err
	}
	body := header[:h-packCRCSize]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(header[h-packCRCSize:]) {
		return nil, errCorruptPack
	}
	frames := make([]packFrame, width)
	end := h
	for i := range frames {
		n := binary.LittleEndian.Uint32(header[packPrefixSize+4*i:])
		if n > maxFrame {
			return nil, errCorruptPack
		}
		frames[i] = packFrame{off: end, n: n}
		end += uint64(n)
	}
	if end == h || end != uint64(size) {
		return nil, errCorruptPack
	}
	return frames, nil
}

type pack struct {
	afero.File
	frames []packFrame
}

func (p *pack) frame(idx uint64) (packFrame, bool) {
	if p == nil || idx >= uint64(len(p.frames)) {
		return packFrame{}, false
	}
	frame := p.frames[idx]
	return frame, frame.n != 0
}

func (p *pack) Close() error {
	if p == nil || p.File == nil {
		return nil
	}
	return p.File.Close()
}

func writeFixed(w io.Writer, b []byte) error {
	_, err := bytes.NewReader(b).WriteTo(w)
	return err
}

func (s *dataColumnStorageImpl) writePackTemp(tmp string, writeFrames func(fh afero.File) ([]uint32, error)) (err error) {
	fh, err := s.fs.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		fh.Close()
		if err != nil {
			s.removeTemp(tmp)
		}
	}()
	if err := writeFixed(fh, make([]byte, packHeaderSize(s.beaconChainConfig.NumberOfColumns))); err != nil {
		return err
	}
	lengths, err := writeFrames(fh)
	if err != nil {
		return err
	}
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := writeFixed(fh, appendPackHeader(nil, lengths)); err != nil {
		return err
	}
	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

func (s *dataColumnStorageImpl) dropFromPack(slot uint64, root common.Hash, drop []bool) error {
	if !slices.Contains(drop, true) {
		return nil
	}
	l := s.forBucket(slot / subdivisionSlot)
	l.RLock()
	defer l.RUnlock()
	pk, err := s.openPack(slot, root)
	if err != nil || pk == nil {
		return err
	}
	defer pk.Close()
	kept := make([]uint32, len(drop))
	dropped := false
	for i, frame := range pk.frames {
		if drop[i] {
			dropped = dropped || frame.n != 0
		} else {
			kept[i] = frame.n
		}
	}
	if !dropped {
		return nil
	}
	_, file := s.packPath(slot, root)
	if !slices.ContainsFunc(kept, func(n uint32) bool { return n != 0 }) {
		pk.Close()
		return s.fs.Remove(file)
	}
	tmp := file + tmpSuffix
	err = s.writePackTemp(tmp, func(fh afero.File) ([]uint32, error) {
		for i, frame := range pk.frames {
			if kept[i] == 0 {
				continue
			}
			if _, err := io.CopyN(fh, io.NewSectionReader(pk, int64(frame.off), int64(frame.n)), int64(frame.n)); err != nil {
				return nil, err
			}
		}
		return kept, nil
	})
	if err != nil {
		return err
	}
	pk.Close()
	if err := s.fs.Rename(tmp, file); err != nil {
		s.removeTemp(tmp)
		return err
	}
	return nil
}

func (s *dataColumnStorageImpl) packable(entries []batchEntry, missing []int) bool {
	return len(missing) >= 2 && s.beaconChainConfig.NumberOfColumns <= math.MaxUint16 &&
		!slices.ContainsFunc(missing, func(i int) bool { return uint64(entries[i].v.EncodingSizeSSZ()) > clparams.MaxPayloadSize })
}

func (s *dataColumnStorageImpl) packPath(slot uint64, root common.Hash) (dir, file string) {
	bucket, _ := s.path(slot, root, 0)
	dir = bucket + "/packs"
	return dir, fmt.Sprintf("%s/%s%s", dir, root.String(), packSuffix)
}

func (s *dataColumnStorageImpl) openPackFile(name string) (*pack, error) {
	fh, err := s.fs.Open(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	info, err := fh.Stat()
	if err != nil {
		fh.Close()
		return nil, err
	}
	frames, err := parsePackHeader(fh, info.Size(), s.beaconChainConfig.NumberOfColumns)
	if err != nil {
		fh.Close()
		return nil, err
	}
	return &pack{File: fh, frames: frames}, nil
}

// A corrupt pack comes back empty: it holds no column, and its presence keeps writes loose.
func (s *dataColumnStorageImpl) openPack(slot uint64, root common.Hash) (*pack, error) {
	_, file := s.packPath(slot, root)
	pk, err := s.openPackFile(file)
	if errors.Is(err, errCorruptPack) {
		lvl := log.LvlTrace
		if _, warned := s.warnedCorruptPacks.LoadOrStore(root, slot/subdivisionSlot); !warned {
			lvl = log.LvlWarn
		}
		log.Log(lvl, "[blob_storage] ignoring corrupt column pack", "slot", slot, "block_root", root.String())
		s.bitmaps.forget(root)
		return &pack{}, nil
	}
	return pk, err
}

func (s *dataColumnStorageImpl) EarliestStoredSlot() (uint64, error) {
	return s.earliestStoredSlot(s.holdsColumn)
}

func (s *dataColumnStorageImpl) holdsColumn(bucket string) (bool, error) {
	if stored, err := s.holdsSidecar(bucket); stored || err != nil {
		return stored, err
	}
	return s.holdsValidPack(bucket + "/packs")
}

func (s *dataColumnStorageImpl) holdsValidPack(dir string) (bool, error) {
	f, err := s.fs.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	var firstErr error
	for {
		names, readErr := f.Readdirnames(listBatch)
		for _, name := range names {
			root, ok := packNameRoot(name)
			if !ok {
				continue
			}
			pk, err := s.openPackFile(dir + "/" + name)
			if errors.Is(err, errCorruptPack) {
				s.bitmaps.forget(root)
				continue
			}
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if pk != nil {
				pk.Close()
				return true, nil
			}
		}
		if errors.Is(readErr, io.EOF) {
			return false, firstErr
		}
		if readErr != nil {
			return false, readErr
		}
	}
}

func packNameRoot(name string) (common.Hash, bool) {
	hex, ok := strings.CutSuffix(name, packSuffix)
	var root common.Hash
	if !ok || root.UnmarshalText([]byte(hex)) != nil || root.String() != hex {
		return common.Hash{}, false
	}
	return root, true
}
