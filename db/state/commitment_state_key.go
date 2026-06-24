package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// CommitmentRootForRange computes the trie root of the commitment file covering steps
// [fromStep,toStep) by loading the root from the existing commitment files (touching a single
// existing key so only the root path is read — not the whole range). It also returns the
// default state-key labels (blockNum, txNum) using the offline-rebuild convention (txNum =
// endTxNum-1). Callers may override the labels (e.g. to the block-boundary convention) before
// building the state value with EncodeCommitmentStateValue.
func CommitmentRootForRange(ctx context.Context, rwDb kv.TemporalRwDB, txNumsReader *rawdbv3.TxNumsReader, fromStep, toStep kv.Step, logger log.Logger) (rootHash []byte, blockNum, txNum uint64, err error) {
	a := rwDb.(HasAgg).Agg().(*Aggregator)
	stepSize := a.StepSize()
	fromTx, toTx := uint64(fromStep)*stepSize, uint64(toStep)*stepSize

	acRo := a.BeginFilesRo()
	defer acRo.Close()

	// one existing key whose path forces the root branch to be read and folded
	oneKey, dom, err := firstKeyInRange(acRo, fromTx, toTx)
	if err != nil {
		return nil, 0, 0, err
	}

	blockNum, err = func() (uint64, error) {
		roTx, err := a.db.BeginRo(ctx)
		if err != nil {
			return 0, err
		}
		defer roTx.Rollback()
		bn, ok, err := txNumsReader.FindBlockNum(ctx, roTx, toTx-1)
		if err != nil {
			return 0, fmt.Errorf("FindBlockNum(%d): %w", toTx-1, err)
		}
		if !ok {
			if bn, _, err = txNumsReader.Last(roTx); err != nil {
				return 0, err
			}
		}
		return bn, nil
	}()
	if err != nil {
		return nil, 0, 0, err
	}

	rwTx, err := rwDb.BeginTemporalRw(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rwTx.Rollback()

	// the shared DB may belong to a live datadir whose commitment tables are populated;
	// clear them in this rolled-back tx so SeekCommitment starts from an empty trie and we
	// load the root purely from the range's files.
	if err := clearCommitmentTables(rwTx); err != nil {
		return nil, 0, 0, err
	}

	domains, err := execctx.NewSharedDomainsWithTrieVariant(ctx, rwTx, logger, commitment.VariantHexPatriciaTrie)
	if err != nil {
		return nil, 0, 0, err
	}
	defer domains.Close()

	// NewSharedDomains' SeekCommitment restored the LATEST commitment state from the shared
	// aggregator's files (the datadir's newest range), which is not the state we want and whose
	// branches don't resolve as-of this range. Drop it so the root is loaded purely from this
	// range's files via the FilesOnlyStateReader below.
	domains.GetCommitmentCtx().Trie().Reset()

	domains.DiscardWrites(kv.AccountsDomain)
	domains.DiscardWrites(kv.StorageDomain)
	domains.DiscardWrites(kv.CodeDomain)
	domains.SetTxNum(toTx - 1)
	domains.GetCommitmentCtx().SetStateReader(commitmentdb.NewFilesOnlyStateReader(rwTx, toTx-1))

	domains.GetCommitmentCtx().TouchKey(dom, string(oneKey), nil)
	rh, err := domains.GetCommitmentCtx().ComputeCommitment(ctx, rwTx, false /* saveState */, blockNum, toTx-1, "", nil)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("ComputeCommitment: %w", err)
	}
	if len(rh) != length.Hash || bytes.Equal(rh, make([]byte, length.Hash)) {
		return nil, 0, 0, fmt.Errorf("computed empty/invalid root %x for range %d-%d", rh, fromStep, toStep)
	}
	logger.Info("[commitment_state_key] computed", "range", fmt.Sprintf("%d-%d", fromStep, toStep), "block", blockNum, "root", common.BytesToHash(rh))
	return bytes.Clone(rh), blockNum, toTx - 1, nil
}

// EncodeCommitmentStateValue wraps a root hash and (blockNum, txNum) into the encoded
// KeyCommitmentState value. The trie state carries only the root cell hash; the full branch
// structure is read from the file on restore.
func EncodeCommitmentStateValue(rootHash []byte, blockNum, txNum uint64) ([]byte, error) {
	trieState, err := commitment.HexTrieEncodeRootState(rootHash)
	if err != nil {
		return nil, err
	}
	v := make([]byte, 18+len(trieState))
	binary.BigEndian.PutUint64(v[0:8], txNum)
	binary.BigEndian.PutUint64(v[8:16], blockNum)
	binary.BigEndian.PutUint16(v[16:18], uint16(len(trieState)))
	copy(v[18:], trieState)
	return v, nil
}

// ComputeCommitmentStateValueForRange recovers the KeyCommitmentState value for the file
// covering steps [fromStep,toStep) using the default (offline-rebuild) labels. This is the
// cheap way to recover the state key for a file that has all its branch data but had the
// state entry stripped (purified).
func ComputeCommitmentStateValueForRange(ctx context.Context, rwDb kv.TemporalRwDB, txNumsReader *rawdbv3.TxNumsReader, fromStep, toStep kv.Step, logger log.Logger) (stateVal []byte, rootHash []byte, blockNumber uint64, err error) {
	rh, blockNum, txNum, err := CommitmentRootForRange(ctx, rwDb, txNumsReader, fromStep, toStep, logger)
	if err != nil {
		return nil, nil, 0, err
	}
	stateVal, err = EncodeCommitmentStateValue(rh, blockNum, txNum)
	if err != nil {
		return nil, nil, 0, err
	}
	return stateVal, rh, blockNum, nil
}

// firstKeyInRange returns the first key (and its domain) from the accounts file for the exact
// range, falling back to storage. The key is guaranteed to exist in the trie so touching it
// only unfolds the root path.
func firstKeyInRange(acRo *AggregatorRoTx, fromTx, toTx uint64) ([]byte, kv.Domain, error) {
	for _, dom := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		st, err := acRo.FileStream(dom, fromTx, toTx)
		if err != nil {
			continue // no file with these exact bounds for this domain
		}
		if st.HasNext() {
			k, _, err := st.Next()
			st.Close()
			if err != nil {
				return nil, 0, err
			}
			return bytes.Clone(k), dom, nil
		}
		st.Close()
	}
	return nil, 0, fmt.Errorf("no account/storage keys found for range %d-%d (txnum)", fromTx, toTx)
}

// RegenerateCommitmentFileWithStateKey streams the source commitment .kv for steps
// [fromStep,toStep) into dstDir, splicing in the KeyCommitmentState entry at its sorted
// position. The branch data is copied verbatim — only the missing state key is added. The
// state value is computed by ComputeCommitmentStateValueForRange. Returns the new file path
// and the root hash. Accessors are NOT built here (caller rebuilds them).
//
// resolveLabel, if non-nil, is called with the recovered root and the default labels
// (blockNum, txNum from the offline-rebuild convention) before the expensive
// streaming/compression. It returns the (blockNum, txNum) to record in the state key — letting
// the caller pick the block-boundary convention when the root matches a block header, or fall
// back to the default mid-block labels otherwise. Returning an error aborts (e.g. the root
// matches no expected header → source branches inconsistent).
func RegenerateCommitmentFileWithStateKey(ctx context.Context, rwDb kv.TemporalRwDB, txNumsReader *rawdbv3.TxNumsReader, fromStep, toStep kv.Step, dstDir string, logger log.Logger, resolveLabel func(rootHash []byte, defBlockNum, defTxNum uint64) (blockNum, txNum uint64, err error)) (dstPath string, rootHash []byte, err error) {
	a := rwDb.(HasAgg).Agg().(*Aggregator)
	fromTx, toTx := uint64(fromStep)*a.StepSize(), uint64(toStep)*a.StepSize()

	acRo := a.BeginFilesRo()
	srcPath := ""
	for _, f := range acRo.Files(kv.CommitmentDomain) {
		if f.StartRootNum() == fromTx && f.EndRootNum() == toTx {
			srcPath = f.Fullpath()
			break
		}
	}
	acRo.Close()
	if srcPath == "" {
		return "", nil, fmt.Errorf("no commitment file for range %d-%d", fromStep, toStep)
	}

	rh, blockNum, txNum, err := CommitmentRootForRange(ctx, rwDb, txNumsReader, fromStep, toStep, logger)
	if err != nil {
		return "", nil, err
	}
	if resolveLabel != nil {
		if blockNum, txNum, err = resolveLabel(rh, blockNum, txNum); err != nil {
			return "", nil, err
		}
	}
	stateVal, err := EncodeCommitmentStateValue(rh, blockNum, txNum)
	if err != nil {
		return "", nil, err
	}

	dstPath = filepath.Join(dstDir, filepath.Base(srcPath))
	if err := streamCommitmentWithStateKey(ctx, a, srcPath, dstPath, fromStep, toStep, stateVal, logger); err != nil {
		return "", nil, err
	}
	return dstPath, rh, nil
}

func streamCommitmentWithStateKey(ctx context.Context, a *Aggregator, srcPath, dstPath string, fromStep, toStep kv.Step, stateVal []byte, logger log.Logger) error {
	cd := a.d[kv.CommitmentDomain]
	compression := seg.CompressNone
	if uint64(toStep-fromStep) > DomainMinStepsToCompress {
		compression = cd.Compression
	}

	decomp, err := seg.NewDecompressor(srcPath)
	if err != nil {
		return err
	}
	defer decomp.Close()
	r := seg.NewReader(decomp.MakeGetter(), compression)
	r.Reset(0)

	comp, err := seg.NewCompressor(ctx, "regen-state-key", dstPath, a.dirs.Tmp, cd.CompressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer comp.Close()
	w := seg.NewWriter(comp, compression)

	totalKV := uint64(decomp.Count()) // keys + values
	stateKey := commitmentdb.KeyCommitmentState
	inserted := false
	var processed uint64
	emit := func(k, v []byte) error {
		if _, err := w.Write(k); err != nil {
			return err
		}
		_, err := w.Write(v)
		processed += 2
		return err
	}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	start := time.Now()
	for r.HasNext() {
		select {
		case <-logEvery.C:
			logger.Info("[commitment_state_key] streaming branches", "dst", filepath.Base(dstPath),
				"at", fmt.Sprintf("%d/%d", processed, totalKV))
		default:
		}
		k, _ := r.Next(nil)
		if !r.HasNext() {
			return fmt.Errorf("dangling key %x in %s", k, srcPath)
		}
		v, _ := r.Next(nil)
		if !inserted {
			switch cmp := bytes.Compare(stateKey, k); {
			case cmp == 0: // already present (not purified) — replace with freshly computed value
				if err := emit(stateKey, stateVal); err != nil {
					return err
				}
				inserted = true
				continue
			case cmp < 0:
				if err := emit(stateKey, stateVal); err != nil {
					return err
				}
				inserted = true
			}
		}
		if err := emit(k, v); err != nil {
			return err
		}
	}
	if !inserted {
		if err := emit(stateKey, stateVal); err != nil {
			return err
		}
	}
	logger.Info("[commitment_state_key] branches streamed, compressing", "dst", filepath.Base(dstPath),
		"words", processed, "spent", time.Since(start))
	if err := comp.Compress(); err != nil {
		return err
	}
	logger.Info("[commitment_state_key] wrote file with state key", "dst", dstPath, "spent", time.Since(start))
	return nil
}
