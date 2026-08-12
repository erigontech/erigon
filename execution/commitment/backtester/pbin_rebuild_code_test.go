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

// The code zone of a rebuilt tree comes from the code domain files alone, so a
// fixture that leaves kv.CodeDomain empty exercises none of it and still passes.
// These tests land real bytecode in those files first.
package backtester_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	pbinCodeStepSize = uint64(8)
	pbinCodeSlots    = 2

	// pbinCodeZoneByte is EIP-8297's CODE zone. A commitment record key is the
	// packed bit path, so a path of at least 8 bits opens with the zone byte.
	pbinCodeZoneByte = 0x01
	// pbinCodeChunkLen is how much code one chunk value holds; byte 0 of the value
	// carries the PUSHDATA count.
	pbinCodeChunkLen = 31
	// pbinCodeGroupChunks is how many chunks share one code-zone stem.
	pbinCodeGroupChunks = 256
)

type pbinCodeAccount struct {
	addr []byte
	code []byte
}

func pbinCodeAddr(i int) []byte {
	a := make([]byte, length.Addr)
	a[0] = byte(i)
	a[length.Addr-1] = byte(i*7 + 1)
	return a
}

// pbinCodeBytes builds PUSH-free bytecode, so no PUSHDATA count crosses a chunk
// boundary and pbinCodeChunkValue can predict every chunk the tree holds.
func pbinCodeBytes(seed byte, n int) []byte {
	code := make([]byte, n)
	for i := range code {
		code[i] = 0x5b // JUMPDEST
	}
	if n > 0 {
		code[0] = seed
	}
	return code
}

// pbinCodeChunkValue is the leaf value chunk i of PUSH-free code carries.
func pbinCodeChunkValue(code []byte, i int) []byte {
	value := make([]byte, 32)
	from := i * pbinCodeChunkLen
	copy(value[1:], code[from:min(from+pbinCodeChunkLen, len(code))])
	return value
}

func pbinCodeAccounts(code []byte, holders ...int) []pbinCodeAccount {
	accts := make([]pbinCodeAccount, 0, len(holders))
	for _, i := range holders {
		accts = append(accts, pbinCodeAccount{addr: pbinCodeAddr(i), code: code})
	}
	return accts
}

// pbinCodeForwardRun writes the fixture accounts, their code and a few storage
// slots for txNums [0, toTx), saving the commitment at every step boundary.
func pbinCodeForwardRun(t *testing.T, db kv.TemporalRwDB, stepSize, toTx uint64, accts []pbinCodeAccount) map[uint64][]byte {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd := pbinM1ABinSharedDomains(t, rwTx)
	defer sd.Close()

	roots := make(map[uint64][]byte)
	for txNum := range toTx {
		for i, a := range accts {
			acc := accounts.Account{
				Nonce:    txNum + 1,
				Balance:  *uint256.NewInt(txNum*1_000 + uint64(i)),
				CodeHash: accounts.EmptyCodeHash,
			}
			if len(a.code) > 0 {
				acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(a.code))
				if txNum == 0 {
					require.NoError(t, sd.DomainPut(kv.CodeDomain, rwTx, a.addr, a.code, txNum, nil))
				}
			}
			prev, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, a.addr)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, a.addr, accounts.SerialiseV3(&acc), txNum, prev))

			for j := range pbinCodeSlots {
				sk := pbinM1ASlotKey(a.addr, j)
				val := []byte{byte(txNum + 1), byte(i + 1), byte(j + 1)}
				prev, _, err := sd.GetLatest(kv.StorageDomain, rwTx, sk)
				require.NoError(t, err)
				require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTx, sk, val, txNum, prev))
			}
		}
		if (txNum+1)%stepSize == 0 {
			root, err := sd.ComputeCommitment(t.Context(), rwTx, true, 0, txNum, "pbin-code", nil)
			require.NoError(t, err)
			require.NotEmpty(t, root)
			roots[txNum] = bytes.Clone(root)
		}
	}
	require.NoError(t, sd.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	return roots
}

// pbinCodeCollatedTxNum is pbinM1ACollatedTxNum plus the code domain, which the
// rebuild reads at the same boundary to chunk an account's code.
func pbinCodeCollatedTxNum(t *testing.T, db kv.TemporalRwDB) uint64 {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := state.AggTx(tx)
	accTxNum := at.TxNumsInFiles(kv.AccountsDomain)
	require.Equal(t, accTxNum, at.TxNumsInFiles(kv.StorageDomain))
	require.Equal(t, accTxNum, at.TxNumsInFiles(kv.CodeDomain),
		"the rebuild reads accounts, storage and code at one boundary")
	return accTxNum
}

// pbinCodeRebuild runs the fixture forward, collates it into domain files, wipes
// the commitment and rebuilds it from those files. It returns the forward root at
// the collated boundary and the rebuilt one.
func pbinCodeRebuild(t *testing.T, accts []pbinCodeAccount, txCount uint64) (kv.TemporalRwDB, []byte, []byte, *state.RebuildReport) {
	t.Helper()
	db, agg, dirs := pbinM1ANewDatadir(t, pbinCodeStepSize)
	stepRoots := pbinCodeForwardRun(t, db, pbinCodeStepSize, txCount, accts)
	require.NoError(t, agg.BuildFiles(txCount))

	collatedTxNum := pbinCodeCollatedTxNum(t, db)
	require.Positive(t, collatedTxNum, "collation must produce domain files to rebuild from")
	wantRoot := stepRoots[collatedTxNum-1]
	require.NotEmpty(t, wantRoot, "the collated boundary must be one the forward run computed a root at")

	db, agg = pbinM1AWipeCommitment(t, db, agg, dirs, pbinCodeStepSize)
	rebuiltRoot, report, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false, state.RebuildTarget{})
	require.NoError(t, err)

	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))
	return db, wantRoot, rebuiltRoot, report
}

// pbinCodeZoneRecords keeps the branch records whose bit path runs through the
// code zone, which is every record the chunk leaves live in.
func pbinCodeZoneRecords(t *testing.T, db kv.TemporalRwDB) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	for k, v := range pbinM1ABranchRecords(t, db) {
		if len(k) >= 2 && k[0] == pbinCodeZoneByte {
			out[k] = v
		}
	}
	return out
}

// pbinCodeReportShards flattens the shards a rebuild reports. Every range walks
// the whole key set at its own boundary, so the code counts are per shard and
// summing them over ranges counts the same account again.
func pbinCodeReportShards(t *testing.T, report *state.RebuildReport) []state.RebuildShardReport {
	t.Helper()
	require.NotNil(t, report)
	var shards []state.RebuildShardReport
	for _, r := range report.Ranges {
		shards = append(shards, r.Shards...)
	}
	require.NotEmpty(t, shards)
	return shards
}

// pbinCodeRecordsHold reports whether some record carries this leaf value. A
// chunk and a delegation indicator are values no state domain holds, so the
// branch record carries them inline.
func pbinCodeRecordsHold(records map[string][]byte, value []byte) bool {
	for _, rec := range records {
		if bytes.Contains(rec, value) {
			return true
		}
	}
	return false
}

func TestPBinRebuildCodeSpansGroups(t *testing.T) {
	pbinM1ABinVariant(t)

	// One chunk past a full group, so the code takes two code-zone stems.
	code := pbinCodeBytes(0x01, (pbinCodeGroupChunks+2)*pbinCodeChunkLen)
	db, wantRoot, rebuiltRoot, _ := pbinCodeRebuild(t, pbinCodeAccounts(code, 1), 4*pbinCodeStepSize)
	require.Equal(t, wantRoot, rebuiltRoot, "a rebuild over code-bearing accounts must reproduce the forward root")

	records := pbinCodeZoneRecords(t, db)
	require.NotEmpty(t, records, "the rebuild must land the account's code in the code zone")
	for _, chunk := range []int{0, pbinCodeGroupChunks - 1, pbinCodeGroupChunks, pbinCodeGroupChunks + 1} {
		require.True(t, pbinCodeRecordsHold(records, pbinCodeChunkValue(code, chunk)),
			"chunk %d must be in the rebuilt code zone", chunk)
	}
}

func TestPBinRebuildSharedCodeChunkedOnce(t *testing.T) {
	pbinM1ABinVariant(t)

	code := pbinCodeBytes(0x02, 3*pbinCodeChunkLen)
	txCount := 4 * pbinCodeStepSize

	soleDB, soleWant, soleGot, soleReport := pbinCodeRebuild(t, pbinCodeAccounts(code, 1), txCount)
	require.Equal(t, soleWant, soleGot)
	sharedDB, sharedWant, sharedGot, sharedReport := pbinCodeRebuild(t, pbinCodeAccounts(code, 1, 2), txCount)
	require.Equal(t, sharedWant, sharedGot)
	require.NotEqual(t, soleWant, sharedWant, "the second holder must change the tree outside the code zone")

	soleZone := pbinCodeZoneRecords(t, soleDB)
	require.NotEmpty(t, soleZone, "the rebuild must land the shared code in the code zone")
	require.Equal(t, soleZone, pbinCodeZoneRecords(t, sharedDB),
		"chunks are addressed by code hash, so a second holder of the same code adds no code-zone record")

	for _, s := range pbinCodeReportShards(t, soleReport) {
		require.Equal(t, uint64(1), s.CodeBearingAccounts)
		require.Equal(t, uint64(1), s.UniqueCodeHashes)
	}
	for _, s := range pbinCodeReportShards(t, sharedReport) {
		require.Equal(t, uint64(2), s.CodeBearingAccounts, "both holders reach the chunker")
		require.Equal(t, uint64(1), s.UniqueCodeHashes, "one shard chunks one code hash once, whoever holds it")
	}
}

func TestPBinRebuildZeroCodeChunkAbsent(t *testing.T) {
	pbinM1ABinVariant(t)

	withZero := bytes.Join([][]byte{
		pbinCodeBytes(0x03, pbinCodeChunkLen),
		make([]byte, pbinCodeChunkLen),
		pbinCodeBytes(0x04, pbinCodeChunkLen),
	}, nil)
	txCount := 4 * pbinCodeStepSize

	zeroDB, zeroWant, zeroGot, _ := pbinCodeRebuild(t, pbinCodeAccounts(withZero, 1), txCount)
	require.Equal(t, zeroWant, zeroGot)
	twoDB, twoWant, twoGot, _ := pbinCodeRebuild(t, pbinCodeAccounts(pbinCodeBytes(0x05, 2*pbinCodeChunkLen), 1), txCount)
	require.Equal(t, twoWant, twoGot)
	threeDB, threeWant, threeGot, _ := pbinCodeRebuild(t, pbinCodeAccounts(pbinCodeBytes(0x06, 3*pbinCodeChunkLen), 1), txCount)
	require.Equal(t, threeWant, threeGot)

	zeroZone := pbinCodeZoneRecords(t, zeroDB)
	require.True(t, pbinCodeRecordsHold(zeroZone, pbinCodeChunkValue(withZero, 0)),
		"the chunk before the zeroed one must survive")
	require.True(t, pbinCodeRecordsHold(zeroZone, pbinCodeChunkValue(withZero, 2)),
		"the chunk after the zeroed one must survive")
	require.Len(t, zeroZone, len(pbinCodeZoneRecords(t, twoDB)),
		"an all-zero chunk leaves the code zone the two surviving chunks alone would build")
	require.Less(t, len(zeroZone), len(pbinCodeZoneRecords(t, threeDB)),
		"three chunks must build a larger code zone, otherwise the count above proves nothing")
}

func TestPBinRebuildDelegatedAccountHasNoCodeLeaves(t *testing.T) {
	pbinM1ABinVariant(t)

	indicator := append([]byte{0xEF, 0x01, 0x00}, pbinCodeAddr(7)...)
	db, wantRoot, rebuiltRoot, delegationReport := pbinCodeRebuild(t, pbinCodeAccounts(indicator, 1), 4*pbinCodeStepSize)
	require.Equal(t, wantRoot, rebuiltRoot)

	require.Empty(t, pbinCodeZoneRecords(t, db), "a delegation indicator is not chunked")
	for _, s := range pbinCodeReportShards(t, delegationReport) {
		require.Zero(t, s.CodeBearingAccounts, "a delegated account holds no code the chunker sees")
		require.Zero(t, s.UniqueCodeHashes)
	}
	leafValue := make([]byte, 32)
	copy(leafValue, indicator)
	require.True(t, pbinCodeRecordsHold(pbinM1ABranchRecords(t, db), leafValue),
		"the rebuilt account must carry the indicator in its DELEGATION leaf")
}

// A range wider than commitment.DefaultRebuildShardMaxSteps is rebuilt in
// several shards, each its own Process over its own slice of the key order. The
// two accounts running the same code sit at the ends of that order, so the
// shard boundary falls between them and the code zone comes out right only if
// the second shard chunks the code rather than taking the first shard's work as
// done.
func TestPBinRebuildSharedCodeAcrossShards(t *testing.T) {
	pbinM1ABinVariant(t)

	// One step per txNum, so the merged accounts file spans 128 steps: twice what
	// a shard covers, in one range. The range must also hold more keys than
	// steps, or the shard loop divides by a zero keys-per-step.
	const (
		stepSize  = uint64(1)
		txCount   = uint64(129)
		acctCount = 50
	)
	code := pbinCodeBytes(0x07, 2*pbinCodeChunkLen)
	accts := make([]pbinCodeAccount, 0, acctCount)
	for i := range acctCount {
		a := pbinCodeAccount{addr: pbinCodeAddr(i)}
		if i == 0 || i == acctCount-1 {
			a.code = code
		}
		accts = append(accts, a)
	}

	db, agg, dirs := pbinM1ANewDatadir(t, stepSize)
	agg.PresetOfflineMerge() // 128 one-step collations, so build them the way the offline tool does
	stepRoots := pbinCodeForwardRun(t, db, stepSize, txCount, accts)
	require.NoError(t, agg.BuildFiles(txCount))

	collatedTxNum := pbinCodeCollatedTxNum(t, db)
	wantRoot := stepRoots[collatedTxNum-1]
	require.NotEmpty(t, wantRoot, "the collated boundary must be one the forward run computed a root at")

	db, agg = pbinM1AWipeCommitment(t, db, agg, dirs, stepSize)

	rebuiltRoot, report, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false, state.RebuildTarget{})
	require.NoError(t, err)
	require.Len(t, report.Ranges, 1)
	require.Len(t, report.Ranges[0].Shards, 2, "the fixture must split one range into two shards, otherwise it proves nothing")
	require.Equal(t, wantRoot, rebuiltRoot)

	for i, s := range pbinCodeReportShards(t, report) {
		require.Positive(t, s.CodeBearingAccounts, "shard %d holds no holder of the shared code, so the split proves nothing", i)
		require.Equal(t, uint64(1), s.UniqueCodeHashes,
			"the chunk cache lives for one shard, so shard %d must chunk the shared code itself", i)
	}

	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))
	records := pbinCodeZoneRecords(t, db)
	require.NotEmpty(t, records)
	for chunk := range 2 {
		require.True(t, pbinCodeRecordsHold(records, pbinCodeChunkValue(code, chunk)),
			"chunk %d of the shared code must be in the rebuilt code zone", chunk)
	}
}
