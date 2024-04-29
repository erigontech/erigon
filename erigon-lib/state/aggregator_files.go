/*
   Copyright 2022 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/metrics"
)

// StepsInBiggestFile - files of this size are completely frozen/immutable.
// files of smaller size are also immutable, but can be removed after merge to bigger files.
const StepsInBiggestFile = 32

var (
	//LatestStateReadWarm          = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="yes"}`)  //nolint
	//LatestStateReadWarmNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="no"}`)   //nolint
	//LatestStateReadGrind         = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="yes"}`) //nolint
	//LatestStateReadGrindNotFound = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="no"}`)  //nolint
	//LatestStateReadCold          = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="yes"}`)  //nolint
	//LatestStateReadColdNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="no"}`)   //nolint
	mxPrunableDAcc  = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="account"}`)
	mxPrunableDSto  = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="storage"}`)
	mxPrunableDCode = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="code"}`)
	mxPrunableDComm = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="commitment"}`)
	mxPrunableHAcc  = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="account"}`)
	mxPrunableHSto  = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="storage"}`)
	mxPrunableHCode = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="code"}`)
	mxPrunableHComm = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="commitment"}`)

	mxRunningMerges        = metrics.GetOrCreateGauge("domain_running_merges")
	mxRunningFilesBuilding = metrics.GetOrCreateGauge("domain_running_files_building")
	mxCollateTook          = metrics.GetOrCreateHistogram("domain_collate_took")
	mxPruneTookDomain      = metrics.GetOrCreateHistogram(`domain_prune_took{type="domain"}`)
	mxPruneTookHistory     = metrics.GetOrCreateHistogram(`domain_prune_took{type="history"}`)
	mxPruneTookIndex       = metrics.GetOrCreateHistogram(`domain_prune_took{type="index"}`)
	mxPruneInProgress      = metrics.GetOrCreateGauge("domain_pruning_progress")
	mxCollationSize        = metrics.GetOrCreateGauge("domain_collation_size")
	mxCollationSizeHist    = metrics.GetOrCreateGauge("domain_collation_hist_size")
	mxPruneSizeDomain      = metrics.GetOrCreateCounter(`domain_prune_size{type="domain"}`)
	mxPruneSizeHistory     = metrics.GetOrCreateCounter(`domain_prune_size{type="history"}`)
	mxPruneSizeIndex       = metrics.GetOrCreateCounter(`domain_prune_size{type="index"}`)
	mxBuildTook            = metrics.GetOrCreateSummary("domain_build_files_took")
	mxStepTook             = metrics.GetOrCreateHistogram("domain_step_took")
	mxFlushTook            = metrics.GetOrCreateSummary("domain_flush_took")
	mxCommitmentRunning    = metrics.GetOrCreateGauge("domain_running_commitment")
	mxCommitmentTook       = metrics.GetOrCreateSummary("domain_commitment_took")
)

type SelectedStaticFilesV3 struct {
	d           [kv.DomainLen][]*filesItem
	dHist       [kv.DomainLen][]*filesItem
	dIdx        [kv.DomainLen][]*filesItem
	logTopics   []*filesItem
	tracesTo    []*filesItem
	tracesFrom  []*filesItem
	logAddrs    []*filesItem
	dI          [kv.DomainLen]int
	logAddrsI   int
	logTopicsI  int
	tracesFromI int
	tracesToI   int
}

func (sf SelectedStaticFilesV3) Close() {
	clist := make([][]*filesItem, 0, kv.DomainLen+4)
	for id := range sf.d {
		clist = append(clist, sf.d[id], sf.dIdx[id], sf.dHist[id])
	}

	clist = append(clist, sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo)
	for _, group := range clist {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.index != nil {
					item.index.Close()
				}
			}
		}
	}
}

func (ac *AggregatorRoTx) staticFilesInRange(r RangesV3) (sf SelectedStaticFilesV3, err error) {
	for id := range ac.d {
		if r.d[id].any() {
			sf.d[id], sf.dIdx[id], sf.dHist[id], sf.dI[id] = ac.d[id].staticFilesInRange(r.d[id])
		}
	}
	if r.logAddrs {
		sf.logAddrs, sf.logAddrsI = ac.logAddrs.staticFilesInRange(r.logAddrsStartTxNum, r.logAddrsEndTxNum)
	}
	if r.logTopics {
		sf.logTopics, sf.logTopicsI = ac.logTopics.staticFilesInRange(r.logTopicsStartTxNum, r.logTopicsEndTxNum)
	}
	if r.tracesFrom {
		sf.tracesFrom, sf.tracesFromI = ac.tracesFrom.staticFilesInRange(r.tracesFromStartTxNum, r.tracesFromEndTxNum)
	}
	if r.tracesTo {
		sf.tracesTo, sf.tracesToI = ac.tracesTo.staticFilesInRange(r.tracesToStartTxNum, r.tracesToEndTxNum)
	}
	return sf, err
}

type MergedFilesV3 struct {
	d          [kv.DomainLen]*filesItem
	dHist      [kv.DomainLen]*filesItem
	dIdx       [kv.DomainLen]*filesItem
	logAddrs   *filesItem
	logTopics  *filesItem
	tracesFrom *filesItem
	tracesTo   *filesItem
}

func (mf MergedFilesV3) FrozenList() (frozen []string) {
	for id, d := range mf.d {
		if d == nil {
			continue
		}
		frozen = append(frozen, d.decompressor.FileName())

		if mf.dHist[id] != nil && mf.dHist[id].frozen {
			frozen = append(frozen, mf.dHist[id].decompressor.FileName())
		}
		if mf.dIdx[id] != nil && mf.dIdx[id].frozen {
			frozen = append(frozen, mf.dIdx[id].decompressor.FileName())
		}
	}

	if mf.logAddrs != nil && mf.logAddrs.frozen {
		frozen = append(frozen, mf.logAddrs.decompressor.FileName())
	}
	if mf.logTopics != nil && mf.logTopics.frozen {
		frozen = append(frozen, mf.logTopics.decompressor.FileName())
	}
	if mf.tracesFrom != nil && mf.tracesFrom.frozen {
		frozen = append(frozen, mf.tracesFrom.decompressor.FileName())
	}
	if mf.tracesTo != nil && mf.tracesTo.frozen {
		frozen = append(frozen, mf.tracesTo.decompressor.FileName())
	}
	return frozen
}
func (mf MergedFilesV3) Close() {
	clist := make([]*filesItem, 0, kv.DomainLen+4)
	for id := range mf.d {
		clist = append(clist, mf.d[id], mf.dHist[id], mf.dIdx[id])
	}
	clist = append(clist, mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo)

	for _, item := range clist {
		if item != nil {
			if item.decompressor != nil {
				item.decompressor.Close()
			}
			if item.index != nil {
				item.index.Close()
			}
		}
	}
}

type MergedFiles struct {
	d     [kv.DomainLen]*filesItem
	dHist [kv.DomainLen]*filesItem
	dIdx  [kv.DomainLen]*filesItem
}

func (mf MergedFiles) FillV3(m *MergedFilesV3) MergedFiles {
	for id := range m.d {
		mf.d[id], mf.dHist[id], mf.dIdx[id] = m.d[id], m.dHist[id], m.dIdx[id]
	}
	return mf
}

func (mf MergedFiles) Close() {
	for id := range mf.d {
		for _, item := range []*filesItem{mf.d[id], mf.dHist[id], mf.dIdx[id]} {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.decompressor != nil {
					item.index.Close()
				}
				if item.bindex != nil {
					item.bindex.Close()
				}
			}
		}
	}
}

func DecodeAccountBytes(enc []byte) (nonce uint64, balance *uint256.Int, hash []byte) {
	balance = new(uint256.Int)

	if len(enc) > 0 {
		pos := 0
		nonceBytes := int(enc[pos])
		pos++
		if nonceBytes > 0 {
			nonce = bytesToUint64(enc[pos : pos+nonceBytes])
			pos += nonceBytes
		}
		balanceBytes := int(enc[pos])
		pos++
		if balanceBytes > 0 {
			balance.SetBytes(enc[pos : pos+balanceBytes])
			pos += balanceBytes
		}
		codeHashBytes := int(enc[pos])
		pos++
		if codeHashBytes > 0 {
			codeHash := make([]byte, length.Hash)
			copy(codeHash, enc[pos:pos+codeHashBytes])
		}
	}
	return
}

func EncodeAccountBytes(nonce uint64, balance *uint256.Int, hash []byte, incarnation uint64) []byte {
	l := 1
	if nonce > 0 {
		l += common.BitLenToByteLen(bits.Len64(nonce))
	}
	l++
	if !balance.IsZero() {
		l += balance.ByteLen()
	}
	l++
	if len(hash) == length.Hash {
		l += 32
	}
	l++
	if incarnation > 0 {
		l += common.BitLenToByteLen(bits.Len64(incarnation))
	}
	value := make([]byte, l)
	pos := 0

	if nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := common.BitLenToByteLen(bits.Len64(nonce))
		value[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			value[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}
	if balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	if len(hash) == 0 {
		value[pos] = 0
		pos++
	} else {
		value[pos] = 32
		pos++
		copy(value[pos:pos+32], hash)
		pos += 32
	}
	if incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := common.BitLenToByteLen(bits.Len64(incarnation))
		value[pos] = byte(incBytes)
		var inc = incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	return value
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}
