package state

import (
	"fmt"
	"hash"
	"os"
	"path/filepath"

	bloomfilter "github.com/holiman/bloomfilter/v2"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/log/v3"
)

type ExistenceFilter struct {
	filter             *bloomfilter.Filter
	empty              bool
	FileName, FilePath string
	f                  *os.File
	noFsync            bool // fsync is enabled by default, but tests can manually disable
}

func NewExistenceFilter(keysCount uint64, filePath string) (*ExistenceFilter, error) {

	m := bloomfilter.OptimalM(keysCount, 0.01)
	//TODO: make filters compatible by usinig same seed/keys
	_, fileName := filepath.Split(filePath)
	e := &ExistenceFilter{FilePath: filePath, FileName: fileName}
	if keysCount < 2 {
		e.empty = true
	} else {
		var err error
		e.filter, err = bloomfilter.New(m)
		if err != nil {
			return nil, fmt.Errorf("%w, %s", err, fileName)
		}
	}
	return e, nil
}

func (b *ExistenceFilter) AddHash(hash uint64) {
	if b.empty {
		return
	}
	b.filter.AddHash(hash)
}
func (b *ExistenceFilter) ContainsHash(v uint64) bool {
	if b.empty {
		return true
	}
	return b.filter.ContainsHash(v)
}
func (b *ExistenceFilter) Contains(v hash.Hash64) bool {
	if b.empty {
		return true
	}
	return b.filter.Contains(v)
}
func (b *ExistenceFilter) Build() error {
	if b.empty {
		cf, err := os.Create(b.FilePath)
		if err != nil {
			return err
		}
		defer cf.Close()
		return nil
	}

	log.Trace("[agg] write file", "file", b.FileName)
	tmpFilePath := b.FilePath + ".tmp"
	cf, err := os.Create(tmpFilePath)
	if err != nil {
		return err
	}
	defer cf.Close()

	if _, err := b.filter.WriteTo(cf); err != nil {
		return err
	}
	if err = b.fsync(cf); err != nil {
		return err
	}
	if err = cf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpFilePath, b.FilePath); err != nil {
		return err
	}
	return nil
}

func (b *ExistenceFilter) DisableFsync() { b.noFsync = true }

// fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (b *ExistenceFilter) fsync(f *os.File) error {
	if b.noFsync {
		return nil
	}
	if err := f.Sync(); err != nil {
		log.Warn("couldn't fsync", "err", err)
		return err
	}
	return nil
}

func OpenExistenceFilter(filePath string) (*ExistenceFilter, error) {
	_, fileName := filepath.Split(filePath)
	f := &ExistenceFilter{FilePath: filePath, FileName: fileName}
	if !dir.FileExist(filePath) {
		return nil, fmt.Errorf("file doesn't exists: %s", fileName)
	}
	{
		ff, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		defer ff.Close()
		stat, err := ff.Stat()
		if err != nil {
			return nil, err
		}
		f.empty = stat.Size() == 0
	}

	if !f.empty {
		var err error
		f.filter, _, err = bloomfilter.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("OpenExistenceFilter: %w, %s", err, fileName)
		}
	}
	return f, nil
}
func (b *ExistenceFilter) Close() {
	if b.f != nil {
		b.f.Close()
		b.f = nil
	}
}
