package checkpoint_sync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/spf13/afero"
)

const finalizedStateRootPrefix = ".finalized-state-root-"

var ErrFinalizedGloasStateRootMissing = errors.New("finalized Gloas state is missing its authoritative state root")

func FinalizedStateRootFileName(snappyState []byte) string {
	digest := sha256.Sum256(snappyState)
	return finalizedStateRootPrefix + hex.EncodeToString(digest[:])
}

func RemoveObsoleteFinalizedStateRoots(directory, keepPath string) error {
	rootFiles, err := filepath.Glob(filepath.Join(directory, finalizedStateRootPrefix+"*"))
	if err != nil {
		return err
	}
	for _, rootPath := range rootFiles {
		if rootPath == keepPath {
			continue
		}
		if err := dir.RemoveFile(rootPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func RestoreFinalizedStateRoot(storage afero.Fs, snappyState []byte, st *state.CachingBeaconState) error {
	if st.Version() < clparams.GloasVersion {
		return nil
	}
	record, err := afero.ReadFile(storage, FinalizedStateRootFileName(snappyState))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			header := st.LatestBlockHeader()
			if st.Version() >= clparams.GloasVersion && header.Slot == st.Slot() && header.Root == (common.Hash{}) {
				return ErrFinalizedGloasStateRootMissing
			}
			return nil
		}
		return fmt.Errorf("read finalized state root: %w", err)
	}
	if len(record) != 2*len(common.Hash{}) {
		return fmt.Errorf("invalid finalized state root record length %d", len(record))
	}
	digest := sha256.Sum256(snappyState)
	checksumInput := make([]byte, 0, len(digest)+len(common.Hash{}))
	checksumInput = append(checksumInput, digest[:]...)
	checksumInput = append(checksumInput, record[:len(common.Hash{})]...)
	wantChecksum := sha256.Sum256(checksumInput)
	if !bytes.Equal(wantChecksum[:], record[len(common.Hash{}):]) {
		return errors.New("invalid finalized state root checksum")
	}
	st.SetPreviousStateRoot(common.BytesToHash(record[:len(common.Hash{})]))
	return nil
}

func EncodeFinalizedStateRoot(snappyState []byte, root common.Hash) []byte {
	digest := sha256.Sum256(snappyState)
	checksumInput := make([]byte, 0, len(digest)+len(root))
	checksumInput = append(checksumInput, digest[:]...)
	checksumInput = append(checksumInput, root[:]...)
	checksum := sha256.Sum256(checksumInput)
	record := make([]byte, 0, 2*len(root))
	record = append(record, root[:]...)
	return append(record, checksum[:]...)
}
