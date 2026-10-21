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

package state

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

type pbinReferenceStampBranch struct {
	path  eip8297.Bitpath
	stamp uint64
}

type pbinReferenceRowStampTracker struct {
	partitions map[pbinPartition]*pbinReferencePartitionStamps
	upper      map[string]uint64
	maximum    uint64
}

type pbinReferencePartitionStamps struct {
	open     []pbinReferenceStampBranch
	closed   map[string]uint64
	previous eip8297.Bitpath
}

func newPBinReferenceRowStampTracker() *pbinReferenceRowStampTracker {
	return &pbinReferenceRowStampTracker{partitions: make(map[pbinPartition]*pbinReferencePartitionStamps), upper: make(map[string]uint64)}
}

func (t *pbinReferenceRowStampTracker) partition(path *eip8297.Bitpath) *pbinReferencePartitionStamps {
	key := pbinPartition{byte(path.Words[0] >> 56), byte(path.Words[0]>>52) & 0x0f}
	stamps := t.partitions[key]
	if stamps == nil {
		stamps = &pbinReferencePartitionStamps{closed: make(map[string]uint64)}
		t.partitions[key] = stamps
	}
	return stamps
}

func (t *pbinReferenceRowStampTracker) observe(key []byte, stamp uint64) error {
	if len(key) == 0 {
		return fmt.Errorf("pbin range writer: empty leaf key")
	}
	path := eip8297.PathFromBits(key, int16(len(key)*8))
	if path.BitLen < pbinPartitionBits {
		return fmt.Errorf("pbin range writer: leaf key %x is shorter than a partition", key)
	}
	for bitLen := int16(4); bitLen < pbinPartitionBits; bitLen += 4 {
		prefix := path
		prefix.Truncate(bitLen)
		name := string(eip8297.EncodeBitPath(&prefix))
		t.upper[name] = max(t.upper[name], stamp)
	}
	t.maximum = max(t.maximum, stamp)
	t.partition(&path).observe(&path, stamp)
	return nil
}

func (p *pbinReferencePartitionStamps) observe(path *eip8297.Bitpath, stamp uint64) {
	p.closeDeeper(eip8297.CommonPrefixBitsAt(&p.previous, 0, path))
	for i := range p.open {
		p.open[i].stamp = max(p.open[i].stamp, stamp)
	}
	start := int16(pbinPartitionBits)
	if len(p.open) > 0 {
		start = p.open[len(p.open)-1].path.BitLen + 4
	}
	for bitLen := start; bitLen <= path.BitLen; bitLen += 4 {
		prefix := *path
		prefix.Truncate(bitLen)
		p.open = append(p.open, pbinReferenceStampBranch{path: prefix, stamp: stamp})
	}
	p.previous = *path
}

func (t *pbinReferenceRowStampTracker) advance(lookahead [][]byte) error {
	live := make(map[*pbinReferencePartitionStamps]struct{}, len(lookahead))
	for _, nextKey := range lookahead {
		if len(nextKey) == 0 {
			return fmt.Errorf("pbin range writer: missing lookahead key")
		}
		nextPath := eip8297.PathFromBits(nextKey, int16(len(nextKey)*8))
		stamps := t.partition(&nextPath)
		stamps.closeDeeper(eip8297.CommonPrefixBitsAt(&stamps.previous, 0, &nextPath))
		live[stamps] = struct{}{}
	}
	for _, stamps := range t.partitions {
		if _, ok := live[stamps]; !ok {
			stamps.closeDeeper(-1)
		}
	}
	return nil
}

func (t *pbinReferenceRowStampTracker) clearClosed() {
	for _, stamps := range t.partitions {
		clear(stamps.closed)
	}
}

func (p *pbinReferencePartitionStamps) closeDeeper(bitLen int16) {
	for len(p.open) > 0 && (bitLen < 0 || p.open[len(p.open)-1].path.BitLen > bitLen) {
		branch := p.open[len(p.open)-1]
		key := string(eip8297.EncodeBitPath(&branch.path))
		p.closed[key] = max(p.closed[key], branch.stamp)
		p.open = p.open[:len(p.open)-1]
	}
}

func (t *pbinReferenceRowStampTracker) stamp(key []byte) (uint64, error) {
	if bytes.Equal(key, pbt.GlobalRootKey()) {
		return t.maximum, nil
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return 0, err
	}
	if path.BitLen < pbinPartitionBits {
		if stamp, ok := t.upper[string(key)]; ok {
			return stamp, nil
		}
		return 0, fmt.Errorf("pbin range writer: row %x has no leaf stamp", key)
	}
	stamps := t.partition(&path)
	if stamp, ok := stamps.closed[string(key)]; ok {
		return stamp, nil
	}
	for i := range stamps.open {
		if stamps.open[i].path == path {
			return stamps.open[i].stamp, nil
		}
	}
	return 0, fmt.Errorf("pbin range writer: row %x has no leaf stamp", key)
}

type pbinCheckedStampTracker struct {
	got     *pbinRowStampTracker
	want    *pbinReferenceRowStampTracker
	lookups *int
}

func (c *pbinCheckedStampTracker) observe(key []byte, stamp uint64) error {
	if err := c.want.observe(key, stamp); err != nil {
		return err
	}
	return c.got.observe(key, stamp)
}

func (c *pbinCheckedStampTracker) advance(lookahead [][]byte) error {
	if err := c.want.advance(lookahead); err != nil {
		return err
	}
	return c.got.advance(lookahead)
}

func (c *pbinCheckedStampTracker) clearClosed() {
	c.want.clearClosed()
	c.got.clearClosed()
}

func (c *pbinCheckedStampTracker) stamp(key []byte) (uint64, error) {
	want, err := c.want.stamp(key)
	if err != nil {
		return 0, err
	}
	got, err := c.got.stamp(key)
	if err != nil {
		return 0, fmt.Errorf("row %x: reference stamp %d, tracker: %w", key, want, err)
	}
	if got != want {
		return 0, fmt.Errorf("row %x: stamp %d, reference stamp %d", key, got, want)
	}
	*c.lookups++
	return got, nil
}
