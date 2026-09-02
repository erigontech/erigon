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

package prune

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
)

var (
	ArchiveMode = Mode{
		Initialised:       true,
		History:           Distance(math.MaxUint64),
		Blocks:            KeepAllBlocksPruneMode,
		CommitmentHistory: KeepAllBlocksPruneMode,
		Receipts:          KeepAllBlocksPruneMode,
	}
	FullMode = Mode{
		Initialised:       true,
		Blocks:            Distance(config3.DefaultPruneDistance),
		History:           Distance(config3.DefaultPruneDistance),
		CommitmentHistory: KeepAllBlocksPruneMode,
		Receipts:          KeepAllBlocksPruneMode,
	}
	BlocksMode = Mode{
		Initialised:       true,
		Blocks:            KeepAllBlocksPruneMode,
		History:           Distance(config3.DefaultPruneDistance),
		CommitmentHistory: KeepAllBlocksPruneMode,
		Receipts:          KeepAllBlocksPruneMode,
	}
	MinimalMode = Mode{
		Initialised:       true,
		Blocks:            Distance(config3.MinimalPruneDistance),
		History:           Distance(config3.MinimalPruneDistance),
		CommitmentHistory: KeepAllBlocksPruneMode,
		Receipts:          KeepAllBlocksPruneMode,
	}
	previousFullMode = Mode{
		Initialised:       true,
		Blocks:            previousDefaultPruneDistance,
		History:           previousDefaultPruneDistance,
		CommitmentHistory: KeepAllBlocksPruneMode,
		Receipts:          KeepAllBlocksPruneMode,
	}

	DefaultMode = ArchiveMode
	MockMode    = Mode{
		Initialised:       true,
		History:           Distance(math.MaxUint64),
		Blocks:            Distance(math.MaxUint64),
		CommitmentHistory: KeepAllBlocksPruneMode,
		Receipts:          KeepAllBlocksPruneMode,
	}

	ErrUnknownPruneMode = fmt.Errorf("--prune.mode must be one of %s, %s, %s, %s", fullModeStr, archiveModeStr, minimalModeStr, blockModeStr)
)

const (
	archiveModeStr               = "archive"
	blockModeStr                 = "blocks"
	fullModeStr                  = "full"
	minimalModeStr               = "minimal"
	previousDefaultPruneDistance = Distance(262_144)
)

type Mode struct {
	Initialised       bool // Set when the values are initialised (not default)
	History           BlockAmount
	Blocks            BlockAmount
	CommitmentHistory BlockAmount
	Receipts          BlockAmount
}

// String describes m in operator-facing output. Current named modes use their
// CLI names, older known shapes use diagnostic labels such as "full(previous)"
// and "full(legacy)", and custom shapes include CLI-style distance overrides.
func (m Mode) String() string {
	if !m.Initialised {
		return archiveModeStr
	}
	// Exact named matches first.
	switch {
	case modeEquals(m, FullMode):
		return fullModeStr
	case modeEquals(m, MinimalMode):
		return minimalModeStr
	case modeEquals(m, BlocksMode):
		return blockModeStr
	case modeEquals(m, ArchiveMode):
		return archiveModeStr
	case modeEquals(m, previousFullMode):
		return fullModeStr + "(previous)"
	}

	// Recognise legacy shapes before rendering generic custom distances.
	if m.Blocks == KeepPostMergeBlocksPruneMode && m.History.Enabled() {
		// Legacy full mode used chain history expiry for blocks and finite state history.
		var sb strings.Builder
		sb.WriteString(fullModeStr + "(legacy)")
		if m.History.toValue() != FullMode.History.toValue() {
			fmt.Fprintf(&sb, " --prune.distance=%s", stateHistoryDistanceCLIValue(m.History.toValue(), KeepPostMergeBlocksPruneMode))
		}
		appendCommitmentHistory(&sb, m)
		appendReceipts(&sb, m)
		return sb.String()
	}
	if m.Blocks == KeepAllBlocksPruneMode && m.History.Enabled() {
		// Blocks-shape (keep all blocks + finite state) but History distance
		// differs from the current BlocksMode default.
		var sb strings.Builder
		sb.WriteString(blockModeStr)
		if m.History.toValue() != BlocksMode.History.toValue() {
			fmt.Fprintf(&sb, " --prune.distance=%s", stateHistoryDistanceCLIValue(m.History.toValue(), KeepPostMergeBlocksPruneMode))
		}
		appendCommitmentHistory(&sb, m)
		appendReceipts(&sb, m)
		return sb.String()
	}

	// Preserve the archive-based rendering for custom shapes.
	var sb strings.Builder
	sb.WriteString(archiveModeStr)
	if m.History.toValue() != DefaultMode.History.toValue() {
		fmt.Fprintf(&sb, " --prune.distance=%s", stateHistoryDistanceCLIValue(m.History.toValue(), KeepPostMergeBlocksPruneMode))
	}
	if m.Blocks.toValue() != DefaultMode.Blocks.toValue() {
		fmt.Fprintf(&sb, " --prune.distance.blocks=%s", blocksDistanceCLIValue(m.Blocks.toValue()))
	}
	appendCommitmentHistory(&sb, m)
	appendReceipts(&sb, m)
	return sb.String()
}

func modeEquals(a, b Mode) bool {
	return a.History.toValue() == b.History.toValue() &&
		a.Blocks.toValue() == b.Blocks.toValue() &&
		commitmentHistoryOrDefault(a.CommitmentHistory).toValue() == commitmentHistoryOrDefault(b.CommitmentHistory).toValue() &&
		receiptsOrDefault(a.Receipts).toValue() == receiptsOrDefault(b.Receipts).toValue()
}

func appendCommitmentHistory(sb *strings.Builder, m Mode) {
	if m.CommitmentHistory != nil && m.CommitmentHistory.toValue() != KeepAllBlocksPruneMode.toValue() {
		fmt.Fprintf(sb, " --prune.commitment-history.distance=%s", stateHistoryDistanceCLIValue(m.CommitmentHistory.toValue(), KeepAllBlocksPruneMode))
	}
}

func appendReceipts(sb *strings.Builder, m Mode) {
	// An unset or KeepAllBlocksPruneMode value is the follow-history default —
	// nothing for the operator to re-pass.
	if m.Receipts == nil || m.Receipts.toValue() == KeepAllBlocksPruneMode.toValue() {
		return
	}
	fmt.Fprintf(sb, " --prune.receipts.distance=%s", stateHistoryDistanceCLIValue(m.Receipts.toValue(), KeepAllReceiptsPruneMode))
}

func FromCli(pruneMode string, distanceHistory, distanceBlocks, commitmentHistoryOlder, receiptsDistance uint64) (Mode, error) {
	var mode Mode
	switch pruneMode {
	case archiveModeStr, "":
		mode = ArchiveMode
	case fullModeStr:
		mode = FullMode
	case minimalModeStr:
		mode = MinimalMode
	case blockModeStr:
		mode = BlocksMode
	default:
		return Mode{}, ErrUnknownPruneMode
	}

	if distanceHistory > 0 {
		mode.History = Distance(distanceHistory)
	}
	if distanceBlocks > 0 {
		mode.Blocks = Distance(distanceBlocks)
	}
	// 0 (or unset) means unlimited: keep the named-mode default rather than
	// Distance(0), which would mean "keep nothing".
	if commitmentHistoryOlder > 0 {
		mode.CommitmentHistory = Distance(commitmentHistoryOlder)
	}
	if receiptsDistance > 0 {
		mode.Receipts = Distance(receiptsDistance)
	}
	return mode, nil
}

// Validate rejects a finite commitment-history window wider than state-history
// retention: commitment history older than --prune.distance can't serve
// eth_getProof, so the excess is wasted. Unbounded windows impose no bound.
func (m Mode) Validate() error {
	commitmentHistory := commitmentHistoryOrDefault(m.CommitmentHistory)
	if !commitmentHistory.Enabled() || m.History == nil || !m.History.Enabled() {
		return nil
	}
	if commitment, history := commitmentHistory.toValue(), m.History.toValue(); commitment > history {
		return fmt.Errorf("--prune.commitment-history.distance=%d exceeds --prune.distance=%d; commitment history older than state-history retention cannot serve eth_getProof", commitment, history)
	}
	return nil
}

func Get(db kv.Getter) (Mode, error) {
	prune := DefaultMode
	prune.Initialised = true

	blockAmount, err := get(db, kv.PruneHistory)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.History = blockAmount
	}

	blockAmount, err = get(db, kv.PruneBlocks)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.Blocks = blockAmount
	}

	blockAmount, err = get(db, kv.PruneCommitmentHistory)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.CommitmentHistory = blockAmount
	}

	blockAmount, err = get(db, kv.PruneReceipts)
	if err != nil {
		return prune, err
	}
	if blockAmount != nil {
		prune.Receipts = blockAmount
	}

	return prune, nil
}

const (
	KeepPostMergeBlocksPruneMode = Distance(math.MaxUint64)     // Use chain-specific history pruning (aka. history-expiry)
	KeepAllBlocksPruneMode       = Distance(math.MaxUint64 - 1) // Keep all history
	// KeepAllReceiptsPruneMode forces the receipt cache to be kept in full.
	// It is distinct from KeepAllBlocksPruneMode, which for receipts is the
	// unset default meaning "follow the state-history window" rather than
	// "keep all" — so an operator needs a separate value to override it.
	KeepAllReceiptsPruneMode = Distance(math.MaxUint64 - 2)
)

type BlockAmount interface {
	PruneTo(stageHead uint64) uint64
	Enabled() bool
	toValue() uint64
	dbType() []byte
}

// Distance amount of blocks to keep in DB
// but manual manipulation with such distance is very unsafe
// for example:
//
//	deleteUntil := currentStageProgress - pruningDistance
//
// may delete whole db - because of uint64 underflow when pruningDistance > currentStageProgress
type Distance uint64

// Enabled reports whether p actively drives distance-based pruning. It is
// false for the sentinel values that select a different policy shape
// (KeepPostMergeBlocksPruneMode → chain history-expiry; KeepAllBlocksPruneMode →
// retain forever / follow-history for receipts; KeepAllReceiptsPruneMode →
// force keep-all receipts) and true for every finite Distance.
func (p Distance) Enabled() bool {
	return p != KeepPostMergeBlocksPruneMode && p != KeepAllBlocksPruneMode && p != KeepAllReceiptsPruneMode
}
func (p Distance) toValue() uint64 { return uint64(p) }
func (p Distance) dbType() []byte  { return kv.PruneTypeOlder }

func (p Distance) PruneTo(stageHead uint64) uint64 {
	if uint64(p) > stageHead {
		return 0
	}
	return stageHead - uint64(p)
}

// EnsureNotChanged initializes prune settings or applies only compatible
// retention changes to an existing datadir.
func EnsureNotChanged(tx kv.GetPut, pruneMode Mode) (Mode, error) {
	if pruneMode.Initialised {
		pruneMode.CommitmentHistory = commitmentHistoryOrDefault(pruneMode.CommitmentHistory)
		pruneMode.Receipts = receiptsOrDefault(pruneMode.Receipts)
		if err := pruneMode.Validate(); err != nil {
			return pruneMode, err
		}
	}
	if err := setIfNotExist(tx, pruneMode); err != nil {
		return pruneMode, err
	}

	pm, err := Get(tx)
	if err != nil {
		return pruneMode, err
	}

	if !pruneMode.Initialised {
		return pm, nil
	}

	// Old archive datadirs stored the history-expiry sentinel for both fields.
	// Treat that pair as the current keep-all archive mode.
	if (pm.History == KeepPostMergeBlocksPruneMode && pruneMode.History == KeepPostMergeBlocksPruneMode) &&
		(pm.Blocks == KeepPostMergeBlocksPruneMode && pruneMode.Blocks == KeepAllBlocksPruneMode) {
		return pruneMode, nil
	}
	if modeEquals(pm, pruneMode) {
		return pm, nil
	}
	if !isRetentionWindowChange(pm, pruneMode) {
		return pm, errors.New("changing --prune.* flags is prohibited, last time you used: --prune.mode=" + pm.String())
	}

	// Compatible window changes are persisted. Widening cannot recover deleted
	// data; narrowing may delete data on the next prune pass.
	log.Warn("[prune] retention window changed from previous run; already-pruned data cannot be recovered",
		"previous", pm.String(), "current", pruneMode.String())
	if err := overwriteStoredMode(tx, pruneMode); err != nil {
		return pruneMode, err
	}
	return pruneMode, nil
}

// isRetentionWindowChange accepts finite History changes, Blocks changes between
// finite retention and chain history expiry, and supported CommitmentHistory or
// Receipts policy changes. Non-finite History changes and Blocks transitions
// involving keep-all remain explicit mode changes.
func isRetentionWindowChange(persisted, requested Mode) bool {
	if modeEquals(persisted, requested) {
		return false
	}
	historyOK := persisted.History == requested.History ||
		(isFiniteDistance(persisted.History) && isFiniteDistance(requested.History))
	blocksOK := persisted.Blocks == requested.Blocks ||
		(isBlocksRetentionPolicy(persisted.Blocks) && isBlocksRetentionPolicy(requested.Blocks))
	commitmentOK := persisted.CommitmentHistory == requested.CommitmentHistory ||
		(isCommitmentHistoryRetentionPolicy(persisted.CommitmentHistory) && isCommitmentHistoryRetentionPolicy(requested.CommitmentHistory))
	receiptsOK := persisted.Receipts == requested.Receipts ||
		(isReceiptsRetentionPolicy(persisted.Receipts) && isReceiptsRetentionPolicy(requested.Receipts))
	return historyOK && blocksOK && commitmentOK && receiptsOK
}

// isCommitmentHistoryRetentionPolicy reports whether b expresses a
// commitment-history retention policy the shim will let operators move between.
// Finite Distance values and KeepAllBlocksPruneMode both qualify: unlike Blocks,
// bounded↔unlimited is accepted in both directions (widening is equivalent to
// widening --prune.distance). KeepPostMergeBlocksPruneMode is meaningless here.
func isCommitmentHistoryRetentionPolicy(b BlockAmount) bool {
	if b == KeepAllBlocksPruneMode {
		return true
	}
	return isFiniteDistance(b)
}

// isReceiptsRetentionPolicy reports whether b expresses a receipt-cache
// retention policy the shim will let operators move between. Finite Distance,
// KeepAllBlocksPruneMode (follow-history default) and KeepAllReceiptsPruneMode
// (force keep-all) all qualify in either direction; KeepPostMergeBlocksPruneMode
// is meaningless here.
func isReceiptsRetentionPolicy(b BlockAmount) bool {
	if b == KeepAllBlocksPruneMode || b == KeepAllReceiptsPruneMode {
		return true
	}
	return isFiniteDistance(b)
}

// isBlocksRetentionPolicy reports whether b expresses a block-data retention
// policy that the shim will let operators move between. Finite Distance values
// and KeepPostMergeBlocksPruneMode (chain-history-expiry) both qualify;
// KeepAllBlocksPruneMode (keep all blocks forever) does not — narrowing from
// "keep all" to anything is a destructive transition that we keep explicit.
func isBlocksRetentionPolicy(b BlockAmount) bool {
	if b == KeepPostMergeBlocksPruneMode {
		return true
	}
	return isFiniteDistance(b)
}

// isFiniteDistance reports whether b is a Distance with a finite retention
// value (i.e., not one of the sentinel values that select a different policy
// shape).
func isFiniteDistance(b BlockAmount) bool {
	d, ok := b.(Distance)
	return ok && d.Enabled()
}

// writeBlockAmount stores one BlockAmount under the given key, replacing any
// existing value. Shared by setOnEmpty (write-if-empty) and overwriteStoredMode
// (unconditional).
func writeBlockAmount(db kv.GetPut, key []byte, b BlockAmount) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, b.toValue())
	if err := db.Put(kv.DatabaseInfo, key, v); err != nil {
		return err
	}
	return db.Put(kv.DatabaseInfo, keyType(key), b.dbType())
}

// commitmentHistoryOrDefault treats an unset CommitmentHistory as keep-all, the
// value every production constructor (FromCli, Get, named modes) already fills
// in; it guards the persistence layer against a nil BlockAmount.
func commitmentHistoryOrDefault(b BlockAmount) BlockAmount {
	if b == nil {
		return KeepAllBlocksPruneMode
	}
	return b
}

// CommitmentHistoryAmount returns the commitment-history retention, treating an
// unset (nil) field as keep-all so callers can query it without a nil check.
func (m Mode) CommitmentHistoryAmount() BlockAmount {
	return commitmentHistoryOrDefault(m.CommitmentHistory)
}

// receiptsOrDefault resolves an unset Receipts to KeepAllBlocksPruneMode — the
// follow-history default (not force keep-all, which is KeepAllReceiptsPruneMode)
// — guarding the persistence layer against a nil BlockAmount.
func receiptsOrDefault(b BlockAmount) BlockAmount {
	if b == nil {
		return KeepAllBlocksPruneMode
	}
	return b
}

// ReceiptsAmount returns the receipt-cache retention, resolving an unset (nil)
// field to the follow-history default so callers can query it without a nil check.
func (m Mode) ReceiptsAmount() BlockAmount {
	return receiptsOrDefault(m.Receipts)
}

// ReceiptsFollowHistory reports whether receipt-cache retention uses the
// follow-history default — no explicit --prune.receipts.distance, so the
// cache tracks the general retention window rather than a finite window or
// KeepAllReceiptsPruneMode.
func (m Mode) ReceiptsFollowHistory() bool {
	return receiptsOrDefault(m.Receipts) == KeepAllBlocksPruneMode
}

func overwriteStoredMode(db kv.GetPut, pm Mode) error {
	if err := writeBlockAmount(db, kv.PruneHistory, pm.History); err != nil {
		return err
	}
	if err := writeBlockAmount(db, kv.PruneBlocks, pm.Blocks); err != nil {
		return err
	}
	if err := writeBlockAmount(db, kv.PruneCommitmentHistory, commitmentHistoryOrDefault(pm.CommitmentHistory)); err != nil {
		return err
	}
	return writeBlockAmount(db, kv.PruneReceipts, receiptsOrDefault(pm.Receipts))
}

func setIfNotExist(db kv.GetPut, pm Mode) error {
	if !pm.Initialised {
		pm = DefaultMode
	}
	if err := setOnEmpty(db, kv.PruneHistory, pm.History); err != nil {
		return err
	}
	if err := setOnEmpty(db, kv.PruneBlocks, pm.Blocks); err != nil {
		return err
	}
	if err := setOnEmpty(db, kv.PruneCommitmentHistory, commitmentHistoryOrDefault(pm.CommitmentHistory)); err != nil {
		return err
	}
	return setOnEmpty(db, kv.PruneReceipts, receiptsOrDefault(pm.Receipts))
}

func createBlockAmount(pruneType []byte, v []byte) (BlockAmount, error) {
	var blockAmount BlockAmount

	switch string(pruneType) {
	case string(kv.PruneTypeOlder):
		blockAmount = Distance(binary.BigEndian.Uint64(v))
	default:
		return nil, fmt.Errorf("unexpected block amount type: %s", string(pruneType))
	}

	return blockAmount, nil
}

func get(db kv.Getter, key []byte) (BlockAmount, error) {
	v, err := db.GetOne(kv.DatabaseInfo, key)
	if err != nil {
		return nil, err
	}

	vType, err := db.GetOne(kv.DatabaseInfo, keyType(key))
	if err != nil {
		return nil, err
	}

	if v != nil {
		blockAmount, err := createBlockAmount(vType, v)
		if err != nil {
			return nil, err
		}
		return blockAmount, nil
	}

	return nil, nil
}

func keyType(name []byte) []byte {
	return append(name, []byte("Type")...)
}

func setOnEmpty(db kv.GetPut, key []byte, blockAmount BlockAmount) error {
	existing, err := db.GetOne(kv.DatabaseInfo, key)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return nil
	}
	return writeBlockAmount(db, key, blockAmount)
}
