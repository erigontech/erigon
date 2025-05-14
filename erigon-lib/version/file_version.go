package version

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Version struct {
	Major uint64
	Minor uint64
}

var ErrVersionIsNotSupported error = errors.New("this version is not supported")

var (
	ZeroVersion            = Version{}
	V1_0          Version  = Version{1, 0}
	V1_1          Version  = Version{1, 1}
	V2_0          Version  = Version{2, 0}
	V1_0_standart Versions = Versions{V1_0, V1_0}
	V1_1_standart Versions = Versions{V1_1, V1_0}
	V2_0_standart Versions = Versions{V2_0, V1_0}
)

func (v Version) Less(rhd Version) bool {
	return v.Major < rhd.Major || v.Major == rhd.Major && v.Minor < rhd.Minor
}

func (v Version) Cmp(rhd Version) int {
	if v.Major < rhd.Major {
		return -1
	} else if v.Major > rhd.Major {
		return 1
	}

	// Majors are equal, compare Minor
	if v.Minor < rhd.Minor {
		return -1
	} else if v.Minor > rhd.Minor {
		return 1
	}

	// Both Major and Minor are equal
	return 0
}

func (v Version) Downgrade() Version {
	if v.Minor == 0 {
		return Version{v.Major - 1, 0}
	}
	return Version{v.Major, v.Minor - 1}
}

func (v Version) IsZero() bool {
	return v.Major == 0 && v.Minor == 0
}

func ParseVersion(v string) (Version, error) {
	if strings.HasPrefix(v, "v") {
		strVersions := strings.Split(v[1:], ".")
		major, err := strconv.ParseUint(strVersions[0], 10, 8)
		if err != nil {
			return Version{}, fmt.Errorf("invalid version: %w", err)
		}
		var minor uint64
		if len(strVersions) > 1 {
			minor, err = strconv.ParseUint(strVersions[1], 10, 8)
			if err != nil {
				return Version{}, fmt.Errorf("invalid version: %w", err)
			}
		} else {
			minor = 0
		}

		return Version{
			Major: major,
			Minor: minor,
		}, nil
	}

	if len(v) == 0 {
		return Version{}, errors.New("invalid version: no prefix")
	}

	return Version{}, fmt.Errorf("invalid version prefix: %s", v[0:1])
}

func (v Version) String() string {
	return fmt.Sprintf("v%d.%d", v.Major, v.Minor)
}

func ReplaceVersion(s string, oldVer, newVer Version) string {
	return strings.ReplaceAll(s, oldVer.String(), newVer.String())
}

type Versions struct {
	Current      Version
	MinSupported Version
}

// To not break existing code. If you want to work somehow with min sup ver, call it.
func (v Versions) IsZero() bool {
	return v.Current.Major == 0 && v.Current.Minor == 0
}

func (v Versions) String() string {
	return v.Current.String()
}

// FindFilesWithVersionsByPattern return an filepath by pattern
func FindFilesWithVersionsByPattern(pattern string) (string, Version, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", Version{}, fmt.Errorf("invalid pattern: %w", err)
	}

	if len(matches) == 0 {
		return "", Version{}, os.ErrNotExist
	}
	if len(matches) > 1 {
		sort.Slice(matches, func(i, j int) bool {
			version1, _ := ParseVersion(matches[i])
			version2, _ := ParseVersion(matches[j])
			return version1.Less(version2)
		})
		ver, _ := ParseVersion(matches[len(matches)-1])
		return matches[len(matches)-1], ver, nil
	}

	ver, _ := ParseVersion(matches[0])
	return matches[0], ver, nil
}

func ReplaceVersionWithMask(s string) (string, error) {
	ver, err := ParseVersion(s)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(s, ver.String(), "*"), nil
}
