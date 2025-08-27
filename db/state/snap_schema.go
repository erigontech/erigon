package state

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// each entitiy has a data_file (e.g. is .seg, .v, .kv; and even .ef for ii), this could be fed to
// seg.Decompressor
// they could also optionally have the following files:
// i) accessor index: essentially recsplit index; e.g kvi, vi, efi, idx
// ii) existence filter: kvei
// iii) bt index: btree index; e.g. .bt

// snapshot name schema holder and parser
// each entity holds one schema.
type SnapNameSchema interface {
	DataTag() string
	AccessorList() statecfg.Accessors
	Parse(filename string) (f *SnapInfo, ok bool)

	// these give out full filepath, not just filename
	DataFile(version statecfg.Version, from, to RootNum) string
	AccessorIdxFile(version statecfg.Version, from, to RootNum, idxPos uint64) string // index or accessor file (recsplit typically)
	BtIdxFile(version statecfg.Version, from, to RootNum) string                      // hack to pass params required for opening btree index
	ExistenceFile(version statecfg.Version, from, to RootNum) string

	AccessorIdxCount() uint64
	DataDirectory() string
	DataFileCompression() seg.FileCompression
}

type _fileMetadata struct {
	folder    string
	supported bool
}

func (f *_fileMetadata) Folder() string  { return f.folder }
func (f *_fileMetadata) Supported() bool { return f.supported }

type BtIdxParams struct {
	Compression seg.FileCompression
}

// per entity schema for e2 entities
type E2SnapSchema struct {
	stepSize uint64

	// tag is the entity "name" used in the snapshot filename.
	dataFileTag   string
	indexFileTags []string

	accessors statecfg.Accessors

	// caches
	dataFileMetadata      *_fileMetadata
	indexFileMetadata     *_fileMetadata
	btIdxFileMetadata     *_fileMetadata
	existenceFileMetadata *_fileMetadata
}

var _ SnapNameSchema = (*E2SnapSchema)(nil)

func NewE2SnapSchema(dirs datadir.Dirs, dataFileTag string) *E2SnapSchema {
	return NewE2SnapSchemaWithStep(dirs, dataFileTag, []string{dataFileTag}, 1000)
}

func NewE2SnapSchemaWithIndexTag(dirs datadir.Dirs, dataFileTag string, indexFileTags []string) *E2SnapSchema {
	return NewE2SnapSchemaWithStep(dirs, dataFileTag, indexFileTags, 1000)
}

func NewE2SnapSchemaWithStep(dirs datadir.Dirs, dataFileTag string, indexFileTags []string, stepSize uint64) *E2SnapSchema {
	return &E2SnapSchema{
		stepSize:      stepSize,
		dataFileTag:   dataFileTag,
		indexFileTags: indexFileTags,
		accessors:     statecfg.AccessorHashMap,

		dataFileMetadata: &_fileMetadata{
			folder:    dirs.Snap,
			supported: true,
		},
		indexFileMetadata: &_fileMetadata{
			folder:    dirs.Snap,
			supported: true,
		},
		btIdxFileMetadata:     &_fileMetadata{},
		existenceFileMetadata: &_fileMetadata{},
	}
}

func (s *E2SnapSchema) DataTag() string {
	return s.dataFileTag
}

func (a *E2SnapSchema) AccessorList() statecfg.Accessors {
	return a.accessors
}

// fileName assumes no folderName in it
func (s *E2SnapSchema) Parse(fileName string) (f *SnapInfo, ok bool) {
	ext := filepath.Ext(fileName)
	if ext != string(DataExtensionSeg) && ext != string(AccessorExtensionIdx) {
		return nil, false
	}
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.SplitN(onlyName, "-", 4)
	res := &SnapInfo{Name: fileName, Ext: ext}

	if len(parts) < 4 {
		return nil, ok
	}

	var err error
	res.Version, err = version.ParseVersion(parts[0])
	if err != nil {
		return res, false
	}

	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return res, false
	}
	res.From = from * s.stepSize
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return res, false
	}
	res.To = to * s.stepSize
	res.FileType = parts[3]
	// it should either match snapshot or its indexes
	if strings.Compare(res.FileType, s.dataFileTag) == 0 {
		return res, true
	} else {
		for _, indexType := range s.indexFileTags {
			if strings.Compare(res.FileType, indexType) == 0 {
				return res, true
			}
		}
		return res, false
	}
}

func (s *E2SnapSchema) DataFile(version statecfg.Version, from, to RootNum) string {
	return filepath.Join(s.dataFileMetadata.folder, fmt.Sprintf("%s-%06d-%06d-%s%s", version, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.dataFileTag, string(DataExtensionSeg)))
}

func (s *E2SnapSchema) AccessorIdxFile(version statecfg.Version, from, to RootNum, idxPos uint64) string {
	return filepath.Join(s.indexFileMetadata.folder, fmt.Sprintf("%s-%06d-%06d-%s%s", version, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.indexFileTags[idxPos], string(AccessorExtensionIdx)))
}

func (s *E2SnapSchema) BtIdxFile(version statecfg.Version, from, to RootNum) string {
	panic("unsupported")
}

func (s *E2SnapSchema) ExistenceFile(version statecfg.Version, from, to RootNum) string {
	panic("unsupported")
}

func (s *E2SnapSchema) DataFileMetadata() *_fileMetadata {
	return s.dataFileMetadata
}

func (s *E2SnapSchema) AccessorIdxFileMetadata() *_fileMetadata {
	return s.indexFileMetadata
}

func (s *E2SnapSchema) BtIdxFileMetadata() *_fileMetadata {
	return s.btIdxFileMetadata
}

func (s *E2SnapSchema) ExistenceFileMetadata() *_fileMetadata {
	return s.existenceFileMetadata
}

func (s *E2SnapSchema) AccessorIdxCount() uint64 {
	return uint64(len(s.indexFileTags))
}

func (s *E2SnapSchema) DataDirectory() string {
	return s.dataFileMetadata.folder
}

func (s *E2SnapSchema) DataFileCompression() seg.FileCompression {
	return seg.CompressNone
}

// E3 Schema

type E3SnapSchema struct {
	stepSize uint64

	dataExtension       DataExtension
	dataFileTag         string
	dataFileCompression seg.FileCompression
	accessors           statecfg.Accessors

	accessorIdxExtension AccessorExtension
	// caches
	dataFileMetadata      *_fileMetadata
	indexFileMetadata     *_fileMetadata
	btIdxFileMetadata     *_fileMetadata
	existenceFileMetadata *_fileMetadata
}

type E3SnapSchemaBuilder struct {
	e *E3SnapSchema
}

func NewE3SnapSchemaBuilder(accessors statecfg.Accessors, stepSize uint64) *E3SnapSchemaBuilder {
	eschema := E3SnapSchemaBuilder{
		e: &E3SnapSchema{},
	}
	e := eschema.e
	e.stepSize = stepSize
	e.accessors = accessors
	return &eschema
}

func (b *E3SnapSchemaBuilder) Data(dataFolder string, dataFileTag string, dataExtension DataExtension, compression seg.FileCompression) *E3SnapSchemaBuilder {
	b.e.dataFileTag = dataFileTag
	b.e.dataExtension = dataExtension
	b.e.dataFileCompression = compression
	b.e.dataFileMetadata = &_fileMetadata{
		folder:    dataFolder,
		supported: true,
	}
	return b
}

// currently assumes dataFolder as the folder
// So, Data() should be called first
func (b *E3SnapSchemaBuilder) BtIndex() *E3SnapSchemaBuilder {
	b.e.btIdxFileMetadata = &_fileMetadata{
		folder:    b.e.dataFileMetadata.folder, // assuming "data" and btindex in same folder, which is currently the case
		supported: true,
	}
	return b
}

func (b *E3SnapSchemaBuilder) Accessor(accessorFolder string) *E3SnapSchemaBuilder {
	b.e.indexFileMetadata = &_fileMetadata{
		folder:    accessorFolder,
		supported: true,
	}

	var ex AccessorExtension
	switch b.e.dataExtension {
	case DataExtensionKv:
		ex = AccessorExtensionKvi
	case DataExtensionV:
		ex = AccessorExtensionVi
	case DataExtensionEf:
		ex = AccessorExtensionEfi
	default:
		panic(fmt.Sprintf("unsupported data extension: %s", b.e.dataExtension))
	}

	b.e.accessorIdxExtension = ex
	return b
}

// Data() should be called first
func (b *E3SnapSchemaBuilder) Existence() *E3SnapSchemaBuilder {
	b.e.existenceFileMetadata = &_fileMetadata{
		folder:    b.e.dataFileMetadata.folder, // assuming "data" and existence in same folder, which is currently the case
		supported: true,
	}
	return b
}

func (b *E3SnapSchemaBuilder) Build() *E3SnapSchema {
	e := b.e
	if e.dataFileMetadata == nil {
		panic("dataFileMetadata not set")
	}

	e.btIdxFileMetadata = b.checkPresence(statecfg.AccessorBTree, e.btIdxFileMetadata)
	e.indexFileMetadata = b.checkPresence(statecfg.AccessorHashMap, e.indexFileMetadata)
	e.existenceFileMetadata = b.checkPresence(statecfg.AccessorExistence, e.existenceFileMetadata)
	return e
}

func (b *E3SnapSchemaBuilder) checkPresence(check statecfg.Accessors, met *_fileMetadata) *_fileMetadata {
	if b.e.accessors&check == 0 && met != nil {
		panic(fmt.Sprintf("accessor %s is not meant to be supported for %s", check, b.e.dataFileTag))
	} else if b.e.accessors&check != 0 && met == nil {
		panic(fmt.Sprintf("accessor %s is meant to be supported for %s", check, b.e.dataFileTag))
	}

	if met == nil {
		met = &_fileMetadata{supported: false}
	}

	return met
}

var _ SnapNameSchema = (*E3SnapSchema)(nil)

var stateFileRegex = regexp.MustCompile("^v([0-9]+).([0-9]+)-([[:lower:]]+).([0-9]+)-([0-9]+).(.*)$")

// fileName assumes no folderName in it
func (s *E3SnapSchema) Parse(fileName string) (f *SnapInfo, ok bool) {
	info := &SnapInfo{Name: fileName}

	subs := stateFileRegex.FindStringSubmatch(fileName)
	if len(subs) != 7 {
		return nil, false
	}

	info.FileType = subs[3]
	if strings.Compare(info.FileType, s.dataFileTag) != 0 {
		return nil, false
	}

	fromStep, err := strconv.ParseUint(subs[4], 10, 64)
	if err != nil {
		return nil, false
	}

	toStep, err := strconv.ParseUint(subs[5], 10, 64)
	if err != nil {
		return nil, false
	}

	info.From = fromStep * s.stepSize
	info.To = toStep * s.stepSize

	info.Version, err = version.ParseVersion(fmt.Sprintf("v%s.%s", subs[1], subs[2]))
	if err != nil {
		return nil, false
	}

	info.Ext = "." + subs[6]

	if s.dataExtension.Equals(info.Ext) {
		return info, true
	} else if s.accessorIdxExtension.Equals(info.Ext) && s.indexFileMetadata.supported {
		return info, true
	} else if info.Ext == ".kvei" && s.existenceFileMetadata.supported {
		return info, true
	} else if info.Ext == ".bt" && s.btIdxFileMetadata.supported {
		return info, true
	}

	return nil, false
}

func (s *E3SnapSchema) DataFile(version statecfg.Version, from, to RootNum) string {
	return filepath.Join(s.dataFileMetadata.folder, fmt.Sprintf("%s-%s.%d-%d%s", version, s.dataFileTag, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.dataExtension))
}

func (s *E3SnapSchema) AccessorIdxFile(version statecfg.Version, from, to RootNum, idxPos uint64) string {
	if !s.indexFileMetadata.supported {
		panic(fmt.Sprintf("%s not supported for %s", statecfg.AccessorHashMap, s.dataFileTag))
	}
	if idxPos > 0 {
		panic("e3 accessor idx pos should be 0")
	}
	return filepath.Join(s.indexFileMetadata.folder, fmt.Sprintf("%s-%s.%d-%d%s", version, s.dataFileTag, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.accessorIdxExtension))
}

func (s *E3SnapSchema) BtIdxFile(version statecfg.Version, from, to RootNum) string {
	if !s.btIdxFileMetadata.supported {
		panic(fmt.Sprintf("%s not supported for %s", statecfg.AccessorBTree, s.dataFileTag))
	}
	return filepath.Join(s.btIdxFileMetadata.folder, fmt.Sprintf("%s-%s.%d-%d.bt", version, s.dataFileTag, from/RootNum(s.stepSize), to/RootNum(s.stepSize)))
}

func (s *E3SnapSchema) ExistenceFile(version statecfg.Version, from, to RootNum) string {
	if !s.existenceFileMetadata.supported {
		panic(fmt.Sprintf("%s not supported for %s", statecfg.AccessorExistence, s.dataFileTag))
	}
	return filepath.Join(s.existenceFileMetadata.folder, fmt.Sprintf("%s-%s.%d-%d.kvei", version, s.dataFileTag, from/RootNum(s.stepSize), to/RootNum(s.stepSize)))
}

func (s *E3SnapSchema) DataTag() string {
	return s.dataFileTag
}

func (s *E3SnapSchema) AccessorList() statecfg.Accessors {
	return s.accessors
}

func (s *E3SnapSchema) AccessorIdxCount() uint64 {
	if !s.indexFileMetadata.supported {
		return 0
	}
	return 1
}

func (s *E3SnapSchema) DataDirectory() string {
	return s.dataFileMetadata.folder
}

func (s *E3SnapSchema) DataFileCompression() seg.FileCompression {
	return s.dataFileCompression
}

// debug method for getting all file extensions for this schema
func (s *E3SnapSchema) FileExtensions() (extensions []string) {
	extensions = append(extensions, s.dataExtension.String())
	if s.indexFileMetadata.supported {
		extensions = append(extensions, s.accessorIdxExtension.String())
	}

	if s.btIdxFileMetadata.supported {
		extensions = append(extensions, ".bt")
	}

	if s.existenceFileMetadata.supported {
		extensions = append(extensions, ".kvei")
	}

	return
}
