// Copyright 2024 The Erigon Authors
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
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Temporary instrumentation for the Normalize read-set experiment. It answers
// one question: when the fill loop falls through to an account read, what did
// the tx's own read set hold for that address, and what did the read return?
var normalizeProbe = dbg.EnvBool("NORMALIZE_PROBE", false)

// skipAbsentDomainRead turns the "recorded absent -> skip the domain" short
// circuit off, so the A/B can be measured without editing code.
var skipAbsentDomainRead = dbg.EnvBool("NORMALIZE_SKIP_ABSENT", true)

type normProbeClass int

const (
	normProbeNotVersioned  normProbeClass = iota // plain domain reader (blockgen / builder)
	normProbeAddrWithVal                         // AddressPath entry carrying an account
	normProbeAddrNilVal                          // AddressPath entry recorded header-only
	normProbeNoAddrTouched                       // no AddressPath entry, other paths read
	normProbeNoAddrCold                          // no read recorded for this address at all
	normProbeClassCount
)

var normProbeClassName = [normProbeClassCount]string{
	"notVersioned", "addrWithVal", "addrNilVal", "noAddrTouched", "noAddrCold",
}

var normProbe struct {
	seen        [normProbeClassCount]atomic.Uint64
	gotAcc      [normProbeClassCount]atomic.Uint64 // the read returned an account
	gotNil      [normProbeClassCount]atomic.Uint64 // the read returned nil
	fromMap     [normProbeClassCount]atomic.Uint64 // versionMap AddressPath answered
	domainReads atomic.Uint64                      // versioned reader reached the domain

	stgFallbacks   atomic.Uint64 // no-op filter reached ReadAccountStorage
	stgSlotInReads atomic.Uint64 // the tx recorded a read of this exact slot
	stgAddrInReads atomic.Uint64 // other slots of this address recorded, not this one
	stgAddrCold    atomic.Uint64 // no storage read recorded for this address
	cacheWritten   atomic.Uint64 // CachedReaderV3 hit an account written this block
	cacheCommitted atomic.Uint64 // ... the decoded committed view
	cacheMiss      atomic.Uint64 // ... nothing cached, went to the domain

	stgDomainReads  atomic.Uint64 // versioned reader reached the domain for a slot
	stgDomainFound  atomic.Uint64 // ... and the slot existed
	stgWriteIsZero  atomic.Uint64 // the write being filtered is zero
	stgFilteredOut  atomic.Uint64 // the fallback concluded "no-op", write dropped
	stgFilteredKeep atomic.Uint64 // the fallback kept the write
}

var normProbeCallers struct {
	sync.Mutex
	m     map[string]uint64
	addrs map[string]map[accounts.Address]uint64
}

// normProbeCaller attributes one domain read to its call site.
func normProbeCaller(probeAddr accounts.Address) {
	var pcs [8]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var site string
	for {
		f, more := frames.Next()
		if !strings.Contains(f.Function, "erigon/execution/state.") {
			site = fmt.Sprintf("%s:%d", f.Function, f.Line)
			break
		}
		if !more {
			break
		}
	}
	normProbeCallers.Lock()
	if normProbeCallers.m == nil {
		normProbeCallers.m = map[string]uint64{}
		normProbeCallers.addrs = map[string]map[accounts.Address]uint64{}
	}
	normProbeCallers.m[site]++
	if normProbeCallers.addrs[site] == nil {
		normProbeCallers.addrs[site] = map[accounts.Address]uint64{}
	}
	normProbeCallers.addrs[site][probeAddr]++
	normProbeCallers.Unlock()
}

func normProbeFallback(reader StateReader, addr accounts.Address) normProbeClass {
	class := normProbeNotVersioned
	if vr, ok := reader.(*versionedStateReader); ok {
		switch {
		case hasAddrRead(vr, addr, true):
			class = normProbeAddrWithVal
		case hasAddrRead(vr, addr, false):
			class = normProbeAddrNilVal
		case vr.reads.touched(addr):
			class = normProbeNoAddrTouched
		default:
			class = normProbeNoAddrCold
		}
		if vr.versionMap != nil {
			if _, res, ok := vr.versionMap.ReadAddress(addr, vr.txIndex); ok && res.Status() == MVReadResultDone {
				normProbe.fromMap[class].Add(1)
			}
		}
	}
	normProbe.seen[class].Add(1)
	return class
}

func hasAddrRead(vr *versionedStateReader, addr accounts.Address, withVal bool) bool {
	tr, ok := vr.reads.GetAddress(addr)
	if !ok {
		return false
	}
	return withVal == (tr.Val != nil && !tr.Val.IsNil())
}

func normProbeResult(class normProbeClass, acc *accounts.Account) {
	if acc != nil {
		normProbe.gotAcc[class].Add(1)
		return
	}
	normProbe.gotNil[class].Add(1)
}

func normProbeStorageFallback(reader StateReader, addr accounts.Address, key accounts.StorageKey, writeIsZero bool) {
	normProbe.stgFallbacks.Add(1)
	if writeIsZero {
		normProbe.stgWriteIsZero.Add(1)
	}
	vr, ok := reader.(*versionedStateReader)
	if !ok {
		return
	}
	switch {
	case hasSlotRead(vr, addr, key):
		normProbe.stgSlotInReads.Add(1)
	case len(vr.reads.storage[addr]) > 0:
		normProbe.stgAddrInReads.Add(1)
	default:
		normProbe.stgAddrCold.Add(1)
	}
}

func hasSlotRead(vr *versionedStateReader, addr accounts.Address, key accounts.StorageKey) bool {
	_, ok := vr.reads.GetStorage(addr, key)
	return ok
}

func normProbeStorageResult(dropped bool) {
	if dropped {
		normProbe.stgFilteredOut.Add(1)
		return
	}
	normProbe.stgFilteredKeep.Add(1)
}

func (s *ReadSet) touched(addr accounts.Address) bool {
	if _, ok := s.balance[addr]; ok {
		return true
	}
	if _, ok := s.nonce[addr]; ok {
		return true
	}
	if _, ok := s.incarnation[addr]; ok {
		return true
	}
	if _, ok := s.codeHash[addr]; ok {
		return true
	}
	if _, ok := s.code[addr]; ok {
		return true
	}
	if _, ok := s.codeSize[addr]; ok {
		return true
	}
	if _, ok := s.selfDestruct[addr]; ok {
		return true
	}
	if _, ok := s.createContract[addr]; ok {
		return true
	}
	_, ok := s.storage[addr]
	return ok
}

// NormalizeProbeDump prints the counters. Called from a last-sorting test file.
func NormalizeProbeDump(label string) {
	var applyLoop uint64
	for c := normProbeAddrWithVal; c < normProbeClassCount; c++ {
		applyLoop += normProbe.seen[c].Load()
	}
	if applyLoop == 0 {
		fmt.Printf("NORMALIZE_PROBE %s: no apply-loop fill-loop fallbacks\n", label)
		return
	}
	fmt.Printf("NORMALIZE_PROBE %s: applyLoop=%d notVersioned=%d versionedReaderDomainReads=%d\n",
		label, applyLoop, normProbe.seen[normProbeNotVersioned].Load(), normProbe.domainReads.Load())
	for c := normProbeAddrWithVal; c < normProbeClassCount; c++ {
		seen := normProbe.seen[c].Load()
		fmt.Printf("NORMALIZE_PROBE %s:   %-14s %6d (%5.1f%%)  gotAccount=%d gotNil=%d viaVersionMap=%d\n",
			label, normProbeClassName[c], seen, 100*float64(seen)/float64(applyLoop),
			normProbe.gotAcc[c].Load(), normProbe.gotNil[c].Load(), normProbe.fromMap[c].Load())
	}
	normProbeCallers.Lock()
	type kv struct {
		k string
		v uint64
	}
	var sites []kv
	for k, v := range normProbeCallers.m {
		sites = append(sites, kv{k, v})
	}
	normProbeCallers.Unlock()
	sort.Slice(sites, func(i, j int) bool { return sites[i].v > sites[j].v })
	for i, s := range sites {
		if i == 8 {
			break
		}
		fmt.Printf("NORMALIZE_PROBE %s:   domainRead site %6d reads / %d distinct addrs  %s\n",
			label, s.v, len(normProbeCallers.addrs[s.k]), s.k)
	}
	fmt.Printf("NORMALIZE_PROBE %s: cachedReader written=%d committed=%d miss=%d\n",
		label, normProbe.cacheWritten.Load(), normProbe.cacheCommitted.Load(), normProbe.cacheMiss.Load())
	stg := normProbe.stgFallbacks.Load()
	if stg == 0 {
		return
	}
	spct := func(v uint64) string { return fmt.Sprintf("%d (%.1f%%)", v, 100*float64(v)/float64(stg)) }
	fmt.Printf("NORMALIZE_PROBE %s: storageFallbacks=%d slotInReads=%s addrInReads=%s addrCold=%s writeIsZero=%s\n",
		label, stg, spct(normProbe.stgSlotInReads.Load()), spct(normProbe.stgAddrInReads.Load()),
		spct(normProbe.stgAddrCold.Load()), spct(normProbe.stgWriteIsZero.Load()))
	fmt.Printf("NORMALIZE_PROBE %s: storageDomainReads=%d found=%d dropped=%d kept=%d\n",
		label, normProbe.stgDomainReads.Load(), normProbe.stgDomainFound.Load(),
		normProbe.stgFilteredOut.Load(), normProbe.stgFilteredKeep.Load())
}
