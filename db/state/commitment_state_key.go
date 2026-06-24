package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// EncodeCommitmentStateValue wraps a root hash and (blockNum, txNum) into the encoded
// KeyCommitmentState value. The trie state carries only the root cell hash; on restore the
// branch structure is read from the file's branches. The root hash is the canonical state
// root for the file's boundary block (i.e. the block header's stateRoot).
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

// WriteCommitmentFileWithStateKey streams the source commitment .kv for steps [fromStep,toStep)
// into dstDir, splicing the KeyCommitmentState entry — built from (rootHash, blockNum, txNum) —
// in at its sorted position. Branch data is copied verbatim; only the state key is added.
//
// rootHash should be the canonical state root for the file's boundary block (the block header's
// stateRoot). The commitment-root recompute is not needed to validate it (CommitmentRoot's
// file-data + SD checks verify the root against the header), and for purified files the
// recompute cannot run anyway — superseded branches (including the root branch) were moved to
// newer files, so run integrity with CHECK_COMMITMENT_ROOT_ONLY_LAST_FILE_RECOMPUTE.
//
// Accessors are NOT built here (caller rebuilds them). Returns the new file path.
func WriteCommitmentFileWithStateKey(ctx context.Context, rwDb kv.TemporalRwDB, fromStep, toStep kv.Step, rootHash []byte, blockNum, txNum uint64, dstDir string, logger log.Logger) (dstPath string, err error) {
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
		return "", fmt.Errorf("no commitment file for range %d-%d", fromStep, toStep)
	}

	stateVal, err := EncodeCommitmentStateValue(rootHash, blockNum, txNum)
	if err != nil {
		return "", err
	}

	dstPath = filepath.Join(dstDir, filepath.Base(srcPath))
	if err := streamCommitmentWithStateKey(ctx, a, srcPath, dstPath, fromStep, toStep, stateVal, logger); err != nil {
		return "", err
	}
	return dstPath, nil
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
			case cmp == 0: // already present — replace with the provided value
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
