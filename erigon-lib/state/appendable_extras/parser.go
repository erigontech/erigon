package entity_extras

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

// snapshot name parser
// owned by SnapshotConfig
type SnapNameParser interface {
	Name() string
	Parse(filename string) (f *SnapInfo, ok bool)

	// these give out full filepath, not just filename
	DataFile(version Version, from, to RootNum) string
	IndexFile(version Version, from, to RootNum, idxPos uint64) string
	BtIdxFile(version Version, from, to RootNum) string
	ExistenceFile(version Version, from, to RootNum) string
}

// v1-000000-000500-bodies.idx, v1-000000-000500-bodies.seg
// v1-000000-000500-transactions-to-block.idx
// per entity parser for e2 entities
type E2Parser struct {
	minAggStep    uint64
	fileType      string
	indexFileType []string
	dir           string
}

var _ SnapNameParser = (*E2Parser)(nil)

func NewE2Parser(dir, fileType string) *E2Parser {
	return NewE2ParserWithStep(1000, dir, fileType, []string{fileType})
}

func NewE2ParserWithStep(minAggStep uint64, dir, fileType string, indexFileType []string) *E2Parser {
	return &E2Parser{minAggStep: minAggStep, fileType: fileType, indexFileType: indexFileType, dir: dir}
}

func (s *E2Parser) Name() string {
	return s.fileType
}

// fileName assumes no folderName in it
func (s *E2Parser) Parse(fileName string) (f *SnapInfo, ok bool) {
	ext := filepath.Ext(fileName)
	if ext != ".seg" && ext != ".idx" {
		return nil, false
	}
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.SplitN(onlyName, "-", 4)
	res := &SnapInfo{Name: fileName, Ext: ext}

	if len(parts) < 4 {
		return nil, ok
	}

	var err error
	res.Version, err = snaptype.ParseVersion(parts[0])
	if err != nil {
		return res, false
	}

	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return res, false
	}
	res.From = from * s.minAggStep
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return res, false
	}
	res.To = to * s.minAggStep
	res.FileType = parts[3]
	// it should either match snapshot or its indexes
	if strings.Compare(res.FileType, s.fileType) == 0 {
		return res, true
	} else {
		for _, indexType := range s.indexFileType {
			if strings.Compare(res.FileType, indexType) == 0 {
				return res, true
			}
		}
		return res, false
	}
}

func (s *E2Parser) DataFile(version Version, from, to RootNum) string {
	return filepath.Join(s.dir, fmt.Sprintf("v%d-%06d-%06d-%s.seg", version, from/RootNum(s.minAggStep), to/RootNum(s.minAggStep), s.fileType))
}

func (s *E2Parser) IndexFile(version Version, from, to RootNum, idxPos uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("v%d-%06d-%06d-%s.idx", version, from/RootNum(s.minAggStep), to/RootNum(s.minAggStep), s.indexFileType[idxPos]))
}

func (s *E2Parser) BtIdxFile(version Version, from, to RootNum) string {
	panic("unsupported")
}

func (s *E2Parser) ExistenceFile(version Version, from, to RootNum) string {
	panic("unsupported")
}
