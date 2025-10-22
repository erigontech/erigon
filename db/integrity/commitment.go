package integrity

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

var errIntegrity = errors.New("integrity error")

func wrapIntegrityError(err error) error {
	if errors.Is(err, errIntegrity) {
		return err
	}
	return fmt.Errorf("%w: %w", errIntegrity, err)
}

func CheckCommitmentKvi(ctx context.Context, db kv.TemporalRoDB, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkKvis(tx, kv.CommitmentDomain, failFast, logger); err != nil {
		return err
	}
	return nil
}

func checkKvis(tx kv.TemporalTx, domain kv.Domain, failFast bool, logger log.Logger) error {
	aggTx := state.AggTx(tx)
	files := aggTx.Files(domain)
	var kvCompression seg.FileCompression
	switch domain {
	case kv.CommitmentDomain:
		kvCompression = statecfg.Schema.CommitmentDomain.Compression
	default:
		panic(fmt.Sprintf("add compression for domain to checkKvis: %s", domain))
	}
	var kvis []state.VisibleFile
	for _, file := range files {
		if strings.HasSuffix(file.Fullpath(), ".kvi") {
			kvis = append(kvis, file)
		}
	}
	slices.SortFunc(kvis, func(a, b state.VisibleFile) int {
		return cmp.Compare(a.EndRootNum(), b.EndRootNum())
	})
	var integrityErr error
	for _, kvi := range kvis {
		err := checkKvi(kvi.Fullpath(), kvCompression, failFast, logger)
		if err == nil {
			continue
		}
		if errors.Is(err, errIntegrity) {
			integrityErr = wrapIntegrityError(err)
			continue
		}
		return err
	}
	return integrityErr
}

func checkKvi(kviFilePath string, kvCompression seg.FileCompression, failFast bool, logger log.Logger) error {
	logger.Trace("checking kvi", "file", kviFilePath)
	kvi, err := recsplit.OpenIndex(kviFilePath)
	if err != nil {
		return err
	}
	defer kvi.Close()
	kviReader := kvi.GetReaderFromPool()
	kvFilePath, ok := strings.CutSuffix(kviFilePath, "i")
	if !ok {
		return fmt.Errorf("invalid kvi file name: %s", kviFilePath)
	}
	kvDecompressor, err := seg.NewDecompressor(kvFilePath)
	if err != nil {
		return err
	}
	defer kvDecompressor.Close()
	kvReader := seg.NewReader(kvDecompressor.MakeGetter(), kvCompression)
	var keyBuf []byte
	var keyOffset uint64
	var integrityErr error
	if uint64(kvReader.Count()) != kvi.KeyCount() {
		err = fmt.Errorf("kv key count %d != kvi key count %d in %s", kvReader.Count(), kvi.KeyCount(), kviFilePath)
		if failFast {
			return err
		} else {
			logger.Warn(err.Error())
		}
	}
	for kvReader.HasNext() {
		keyBuf, keyOffset = kvReader.Next(keyBuf[:0])
		logger.Trace("checking kvi for", "key", keyBuf, "offset", keyOffset)
		kviOffset, ok := kviReader.Lookup(keyBuf)
		if !ok {
			err = fmt.Errorf("key %x not found in %s", keyBuf, kviFilePath)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = wrapIntegrityError(err)
			}
		}
		if kviOffset != keyOffset {
			err = fmt.Errorf("key %x offset mismatch in %s: %d != %d", keyBuf, kviFilePath, keyOffset, kviOffset)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = wrapIntegrityError(err)
			}
		}
	}
	return integrityErr
}
