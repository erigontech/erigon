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

// "snapshot files" name schema holder and parser
// each entity has one schema.
type SnapNameSchema interface {
	DataTag() string
	IndexTags() []string
	AccessorList() statecfg.Accessors
	Parse(baseFileName string) (f *SnapInfo, ok bool)

	// these give out full filepath, not just filename
	// if version.IsZero(), then current version is used
	// if version.IsSearch() or IsStrictSearch(), then directory is searched for existing file (will try to find any version in supported range)
	// err might be returned only when search version is used...
	DataFile(version statecfg.Version, from, to RootNum) (filename string, err error)
	AccessorIdxFile(version statecfg.Version, from, to RootNum, idxPos uint16) (filename string, err error) // index or accessor file (recsplit typically)
	BtIdxFile(version statecfg.Version, from, to RootNum) (filename string, err error)
	ExistenceFile(version statecfg.Version, from, to RootNum) (filename string, err error)

	// metadata
	AccessorIdxCount() uint16
	DataDirectory() string

	// hack, ideally doesn't belong here but
	// is self contained info in file
	DataFileCompression() seg.FileCompression
}

type _fileMetadata struct {
	folder    string
	supported bool
}

func (f *_fileMetadata) Folder() string  { return f.folder }
func (f *_fileMetadata) Supported() bool { return f.supported }

// per entity schema for e2 entities
type E2SnapSchema struct {
	stepSize uint64

	// tag is the entity "name" used in the snapshot filename.
	dataFileTag   string
	indexFileTags []string

	accessors      statecfg.Accessors
	currentVersion E2SnapSchemaVersion

	// caches
	dataFileMetadata      *_fileMetadata
	indexFileMetadata     *_fileMetadata
	btIdxFileMetadata     *_fileMetadata
	existenceFileMetadata *_fileMetadata
}

var E2_FILE_TEMPLATE = "%s-%06d-%06d-%s%s"

type E2SnapSchemaVersion struct {
	DataFileVersion version.Versions
	AccessorVersion version.Versions
}

func NewE2SnapSchemaVersion(dataVer, accessorVer version.Versions) E2SnapSchemaVersion {
	return E2SnapSchemaVersion{
		DataFileVersion: dataVer,
		AccessorVersion: accessorVer,
	}
}

var _ SnapNameSchema = (*E2SnapSchema)(nil)

func NewE2SnapSchema(dirs datadir.Dirs, dataFileTag string, currentVersion E2SnapSchemaVersion) *E2SnapSchema {
	return NewE2SnapSchemaWithStep(dirs, dataFileTag, []string{dataFileTag}, 1000, currentVersion)
}

func NewE2SnapSchemaWithIndexTag(dirs datadir.Dirs, dataFileTag string, indexFileTags []string, currentVersion E2SnapSchemaVersion) *E2SnapSchema {
	return NewE2SnapSchemaWithStep(dirs, dataFileTag, indexFileTags, 1000, currentVersion)
}

func NewE2SnapSchemaWithStep(dirs datadir.Dirs, dataFileTag string, indexFileTags []string, stepSize uint64, currentVersion E2SnapSchemaVersion) *E2SnapSchema {
	return NewE2SnapSchemaWithStepAndDir(dirs.Snap, dataFileTag, indexFileTags, stepSize, currentVersion)
}

func NewE2SnapSchemaWithStepAndDir(dir string, dataFileTag string, indexFileTags []string, stepSize uint64, currentVersion E2SnapSchemaVersion) *E2SnapSchema {
	return &E2SnapSchema{
		stepSize:       stepSize,
		dataFileTag:    dataFileTag,
		indexFileTags:  indexFileTags,
		accessors:      statecfg.AccessorHashMap,
		currentVersion: currentVersion,

		dataFileMetadata: &_fileMetadata{
			folder:    dir,
			supported: true,
		},
		indexFileMetadata: &_fileMetadata{
			folder:    dir,
			supported: true,
		},
		btIdxFileMetadata:     &_fileMetadata{},
		existenceFileMetadata: &_fileMetadata{},
	}
}

func (s *E2SnapSchema) DataTag() string {
	return s.dataFileTag
}

func (s *E2SnapSchema) IndexTags() []string {
	return s.indexFileTags
}

func (a *E2SnapSchema) AccessorList() statecfg.Accessors {
	return a.accessors
}

// fileName assumes no folderName in it
func (s *E2SnapSchema) Parse(baseFileName string) (f *SnapInfo, ok bool) {
	ext := filepath.Ext(baseFileName)
	if ext != string(DataExtensionSeg) && ext != string(AccessorExtensionIdx) {
		return nil, false
	}
	onlyName := baseFileName[:len(baseFileName)-len(ext)]
	parts := strings.SplitN(onlyName, "-", 4)
	res := &SnapInfo{Name: baseFileName, Ext: ext}

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

func (s *E2SnapSchema) DataFile(filev statecfg.Version, from, to RootNum) (string, error) {
	if filev.IsZero() {
		filev = s.currentVersion.DataFileVersion.Current
	}
	if !filev.IsSearch() {
		return filepath.Join(s.dataFileMetadata.folder, fmt.Sprintf(E2_FILE_TEMPLATE, filev, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.dataFileTag, string(DataExtensionSeg))), nil
	}

	pattern := s.fileFormat("*", from, to)
	return findFilesWithVersionsByPattern(filev, pattern, s.currentVersion.DataFileVersion, s.Parse)
}

func (s *E2SnapSchema) AccessorIdxFile(filev statecfg.Version, from, to RootNum, idxPos uint16) (string, error) {
	if filev.IsZero() {
		filev = s.currentVersion.AccessorVersion.Current
	}
	if !filev.IsSearch() {
		return filepath.Join(s.indexFileMetadata.folder, fmt.Sprintf(E2_FILE_TEMPLATE, filev, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.indexFileTags[idxPos], string(AccessorExtensionIdx))), nil
	}

	// search for index file in directory
	pattern := s.idxFileFormat("*", from, to, idxPos)
	return findFilesWithVersionsByPattern(filev, pattern, s.currentVersion.AccessorVersion, s.Parse)
}

func (s *E2SnapSchema) fileFormat(version string, from, to RootNum) string {
	basefile := fmt.Sprintf(E2_FILE_TEMPLATE, version, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.dataFileTag, string(DataExtensionSeg))
	return filepath.Join(s.dataFileMetadata.folder, basefile)
}

func (s *E2SnapSchema) idxFileFormat(version string, from, to RootNum, idxPos uint16) string {
	basefile := fmt.Sprintf(E2_FILE_TEMPLATE, version, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.indexFileTags[idxPos], string(AccessorExtensionIdx))
	return filepath.Join(s.indexFileMetadata.folder, basefile)
}

func (s *E2SnapSchema) BtIdxFile(version statecfg.Version, from, to RootNum) (string, error) {
	panic("unsupported")
}

func (s *E2SnapSchema) ExistenceFile(version statecfg.Version, from, to RootNum) (string, error) {
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

func (s *E2SnapSchema) AccessorIdxCount() uint16 {
	return uint16(len(s.indexFileTags))
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

	currentVersion E3SnapSchemaVersion
}

type E3SnapSchemaVersion struct {
	DataFileVersion    version.Versions
	AccessorIdxVersion version.Versions
	BtIdxVersion       version.Versions
	ExistenceVersion   version.Versions
}

type E3SnapSchemaBuilder struct {
	e *E3SnapSchema
}

func SnapSchemaFromDomainCfg(cfg statecfg.DomainCfg, dirs datadir.Dirs, stepSize uint64) (domain, history, ii *E3SnapSchema) {
	domain = NewDomainSnapSchema(cfg, stepSize, dirs)

	if cfg.Hist.HistoryDisabled {
		return
	}

	history = NewHistorySnapSchema(cfg.Hist, stepSize, dirs)
	ii = NewIISnapSchema(cfg.Hist.IiCfg, stepSize, dirs)
	return
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

func (b *E3SnapSchemaBuilder) Data(dataFolder string, fileTag string, ext DataExtension, compression seg.FileCompression, version version.Versions) *E3SnapSchemaBuilder {
	b.e.dataFileTag = fileTag
	b.e.dataExtension = ext
	b.e.dataFileCompression = compression
	b.e.dataFileMetadata = &_fileMetadata{
		folder:    dataFolder,
		supported: true,
	}
	b.e.currentVersion.DataFileVersion = version
	return b
}

// currently assumes dataFolder as the folder
// So, Data() should be called first
func (b *E3SnapSchemaBuilder) BtIndex(version version.Versions) *E3SnapSchemaBuilder {
	b.e.btIdxFileMetadata = &_fileMetadata{
		folder:    b.e.dataFileMetadata.folder, // assuming "data" and btindex in same folder, which is currently the case
		supported: true,
	}
	b.e.currentVersion.BtIdxVersion = version
	return b
}

func (b *E3SnapSchemaBuilder) Accessor(accessorFolder string, version version.Versions) *E3SnapSchemaBuilder {
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
	b.e.currentVersion.AccessorIdxVersion = version
	return b
}

// Data() should be called first
func (b *E3SnapSchemaBuilder) Existence(version version.Versions) *E3SnapSchemaBuilder {
	b.e.existenceFileMetadata = &_fileMetadata{
		folder:    b.e.dataFileMetadata.folder, // assuming "data" and existence in same folder, which is currently the case
		supported: true,
	}
	b.e.currentVersion.ExistenceVersion = version
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
func (s *E3SnapSchema) Parse(baseFileName string) (f *SnapInfo, ok bool) {
	info := &SnapInfo{Name: baseFileName}

	subs := stateFileRegex.FindStringSubmatch(baseFileName)
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

func (s *E3SnapSchema) DataFile(filev statecfg.Version, from, to RootNum) (string, error) {
	if filev.IsZero() {
		filev = s.currentVersion.DataFileVersion.Current
	}
	if !filev.IsSearch() {
		return filepath.Join(s.dataFileMetadata.folder, fmt.Sprintf("%s-%s.%d-%d%s", filev, s.dataFileTag, from/RootNum(s.stepSize), to/RootNum(s.stepSize), s.dataExtension)), nil
	}

	pattern := s.fileFormat(s.dataFileMetadata.folder, "*", from, to, string(s.dataExtension))
	return findFilesWithVersionsByPattern(filev, pattern, s.currentVersion.DataFileVersion, s.Parse)
}

func (s *E3SnapSchema) AccessorIdxFile(filev statecfg.Version, from, to RootNum, idxPos uint16) (string, error) {
	if !s.indexFileMetadata.supported {
		panic(fmt.Sprintf("%s not supported for %s", statecfg.AccessorHashMap, s.dataFileTag))
	}
	if idxPos > 0 {
		panic("e3 accessor idx pos should be 0")
	}
	if filev.IsZero() {
		filev = s.currentVersion.AccessorIdxVersion.Current
	}
	if !filev.IsSearch() {
		return s.fileFormat(s.indexFileMetadata.folder, filev.String(), from, to, string(s.accessorIdxExtension)), nil
	}

	basefile := s.fileFormat(s.indexFileMetadata.folder, "*", from, to, string(s.accessorIdxExtension))
	return findFilesWithVersionsByPattern(filev, basefile, s.currentVersion.AccessorIdxVersion, s.Parse)
}

func (s *E3SnapSchema) fileFormat(folder string, version string, from, to RootNum, ext string) string {
	basefile := fmt.Sprintf("%s-%s.%d-%d%s", version, s.dataFileTag, from/RootNum(s.stepSize), to/RootNum(s.stepSize), ext)
	return filepath.Join(folder, basefile)
}

func (s *E3SnapSchema) BtIdxFile(filev statecfg.Version, from, to RootNum) (string, error) {
	if !s.btIdxFileMetadata.supported {
		panic(fmt.Sprintf("%s not supported for %s", statecfg.AccessorBTree, s.dataFileTag))
	}
	if filev.IsZero() {
		filev = s.currentVersion.BtIdxVersion.Current
	}
	if !filev.IsSearch() {
		return s.fileFormat(s.btIdxFileMetadata.folder, filev.String(), from, to, ".bt"), nil
	}
	basefile := s.fileFormat(s.btIdxFileMetadata.folder, "*", from, to, ".bt")
	return findFilesWithVersionsByPattern(filev, basefile, s.currentVersion.BtIdxVersion, s.Parse)
}

func (s *E3SnapSchema) ExistenceFile(filev statecfg.Version, from, to RootNum) (string, error) {
	if !s.existenceFileMetadata.supported {
		panic(fmt.Sprintf("%s not supported for %s", statecfg.AccessorExistence, s.dataFileTag))
	}
	if filev.IsZero() {
		filev = s.currentVersion.ExistenceVersion.Current
	}
	if !filev.IsSearch() {
		return s.fileFormat(s.existenceFileMetadata.folder, filev.String(), from, to, ".kvei"), nil
	}
	basefile := s.fileFormat(s.existenceFileMetadata.folder, "*", from, to, ".kvei")
	return findFilesWithVersionsByPattern(filev, basefile, s.currentVersion.ExistenceVersion, s.Parse)
}

func (s *E3SnapSchema) DataTag() string {
	return s.dataFileTag
}

func (s *E3SnapSchema) IndexTags() []string {
	return []string{s.dataFileTag}
}

func (s *E3SnapSchema) AccessorList() statecfg.Accessors {
	return s.accessors
}

func (s *E3SnapSchema) AccessorIdxCount() uint16 {
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

// these are v + vi or bt/kvei residing in same folder `snapshots/forkables`
func NewForkableSnapSchema(cfg statecfg.ForkableCfg, stepSize uint64, dirs datadir.Dirs) SnapNameSchema {
	b := NewE3SnapSchemaBuilder(cfg.Accessors, stepSize)
	b.Data(dirs.SnapForkable, cfg.Name, DataExtensionV, cfg.Compression, version.V1_1_exact)
	if cfg.Accessors&statecfg.AccessorBTree != 0 {
		b.BtIndex(version.V1_1_exact)
	}
	if cfg.Accessors&statecfg.AccessorHashMap != 0 {
		b.Accessor(dirs.SnapForkable, version.V1_1_exact)
	}
	if cfg.Accessors&statecfg.AccessorExistence != 0 {
		b.Existence(version.V1_1_exact)
	}

	return b.Build()
}

func NewDomainSnapSchema(cfg statecfg.DomainCfg, stepSize uint64, dirs datadir.Dirs) *E3SnapSchema {
	b := NewE3SnapSchemaBuilder(cfg.Accessors, stepSize).
		Data(dirs.SnapDomain, cfg.Name.String(), DataExtensionKv, cfg.Compression, cfg.FileVersion.DataKV)

	if cfg.Accessors.Has(statecfg.AccessorBTree) {
		b.BtIndex(cfg.FileVersion.AccessorBT)
	}
	if cfg.Accessors.Has(statecfg.AccessorHashMap) {
		// kvi in same folder
		b.Accessor(dirs.SnapDomain, cfg.FileVersion.AccessorKVI)
	}
	if cfg.Accessors.Has(statecfg.AccessorExistence) {
		b.Existence(cfg.FileVersion.AccessorKVEI)
	}

	return b.Build()
}

func NewHistorySnapSchema(cfg statecfg.HistCfg, stepSize uint64, dirs datadir.Dirs) *E3SnapSchema {
	b := NewE3SnapSchemaBuilder(cfg.Accessors, stepSize).
		Data(dirs.SnapHistory, cfg.HistoryIdx.String(), DataExtensionV, cfg.Compression, cfg.FileVersion.DataV)

	if cfg.Accessors.Has(statecfg.AccessorHashMap) {
		b.Accessor(dirs.SnapAccessors, cfg.FileVersion.AccessorVI)
	}

	return b.Build()
}

func NewIISnapSchema(cfg statecfg.InvIdxCfg, stepSize uint64, dirs datadir.Dirs) *E3SnapSchema {
	b := NewE3SnapSchemaBuilder(cfg.Accessors, stepSize).
		Data(dirs.SnapIdx, cfg.Name.String(), DataExtensionEf, cfg.Compression, cfg.FileVersion.DataEF)
	if cfg.Accessors.Has(statecfg.AccessorHashMap) {
		b.Accessor(dirs.SnapAccessors, cfg.FileVersion.AccessorEFI)
	}

	return b.Build()
}

// fullpath pattern
func findFilesWithVersionsByPattern(searchVer version.Version, pattern string, supported version.Versions, parseOp func(filename string) (*SnapInfo, bool)) (string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		panic(fmt.Sprintf("invalid pattern: %s, err: %v", pattern, err))
	}

	isStrict := searchVer.Eq(version.StrictSearchVersion)

	if len(matches) == 0 {
		return "", fmt.Errorf("no match found for pattern %s", pattern)
	}
	if isStrict && len(matches) > 1 {
		return "", fmt.Errorf("more than one match found for pattern: %s", pattern)
	}
	maxVersion := version.ZeroVersion
	maxMatch := ""
	for _, match := range matches {
		filename := filepath.Base(match)
		info, ok := parseOp(filename)
		if !ok {
			panic(fmt.Sprintf("match %s can't be parsed, shouldn't happen, fail fast", filename))
		}
		if info.Version.GreaterOrEqual(supported.MinSupported) && info.Version.LessOrEqual(supported.Current) && maxVersion.Less(info.Version) {
			maxVersion = info.Version
			maxMatch = match
			continue
		}
		if isStrict {
			return "", fmt.Errorf("can't parse file (strict=true) %s", match)
		}
	}
	if maxVersion.IsZero() {
		return "", fmt.Errorf("couldn't find parseable file for pattern %s", pattern)
	}
	return maxMatch, nil
}
