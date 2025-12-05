package reset

// Type wrappers to ensure valid runtime values.

import (
	"path/filepath"

	"github.com/anacrolix/missinggo/v2/panicif"
)

type (
	// A confirmed OS file-path.
	OsFilePath string
	// A valid slash name.
	slashName string
)

func (me OsFilePath) MustLocalRelSlash(base OsFilePath) slashName {
	rel, err := filepath.Rel(string(base), string(me))
	panicif.Err(err)
	panicif.False(filepath.IsLocal(rel))
	return slashName(filepath.ToSlash(rel))
}

func (me OsFilePath) MustRel(base OsFilePath) OsFilePath {
	rel, err := filepath.Rel(string(base), string(me))
	panicif.Err(err)
	return OsFilePath(rel)
}

func (me OsFilePath) Join(other OsFilePath) OsFilePath {
	return OsFilePath(filepath.Join(string(me), string(other)))
}

// Joins two paths, preferring the latter if it's absolute. Common behaviour in other languages.
func (me OsFilePath) JoinClobbering(other OsFilePath) OsFilePath {
	if filepath.IsAbs(string(other)) {
		return other
	}
	return OsFilePath(filepath.Join(string(me), string(other)))
}

func (me OsFilePath) ToString() string {
	return string(me)
}

func (me slashName) MustLocalize() OsFilePath {
	fp, err := filepath.Localize(string(me))
	panicif.Err(err)
	return OsFilePath(fp)
}

func (me slashName) FromSlash() OsFilePath {
	return OsFilePath(filepath.FromSlash(string(me)))
}
