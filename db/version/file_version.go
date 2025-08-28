package version

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
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
	V1_2          Version  = Version{1, 2}
	V2_0          Version  = Version{2, 0}
	V2_1          Version  = Version{2, 1}
	V1_0_standart Versions = Versions{V1_0, V1_0}
	V1_1_standart Versions = Versions{V1_1, V1_0}
	V1_2_standart Versions = Versions{V1_2, V1_0}
	V1_1_exact    Versions = Versions{V1_1, V1_1}
	V2_0_standart Versions = Versions{V2_0, V1_0}
	V2_1_standart Versions = Versions{V2_1, V1_0}
)

func (v Version) Less(rhd Version) bool {
	return v.Major < rhd.Major || v.Major == rhd.Major && v.Minor < rhd.Minor
}

func (v Version) Greater(rhd Version) bool {
	return !v.Less(rhd) && !v.Eq(rhd)
}

func (v Version) LessOrEqual(rhd Version) bool {
	return !v.Greater(rhd)
}

func (v Version) GreaterOrEqual(rhd Version) bool {
	return !v.Less(rhd)
}

func (v Version) BumpMinor() Version {
	return Version{v.Major, v.Minor + 1}
}

func (v Version) BumpMajor() Version {
	return Version{v.Major + 1, 0}
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

func (v Version) Eq(rhd Version) bool {
	if v.Major == rhd.Major {
		if v.Minor == rhd.Minor {
			return true
		}
	}
	return false
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
		versionString := strings.Split(v, "-")[0]
		strVersions := strings.Split(versionString[1:], ".")
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
func FindFilesWithVersionsByPattern(pattern string) (string, Version, bool, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", Version{}, false, fmt.Errorf("invalid pattern: %w", err)
	}

	if len(matches) == 0 {
		return "", Version{}, false, nil
	}
	if len(matches) > 1 {
		sort.Slice(matches, func(i, j int) bool {
			_, fName1 := filepath.Split(matches[i])
			version1, _ := ParseVersion(fName1)

			_, fName2 := filepath.Split(matches[j])
			version2, _ := ParseVersion(fName2)

			return version1.Less(version2)
		})
		_, fName := filepath.Split(matches[len(matches)-1])
		ver, _ := ParseVersion(fName)

		return matches[len(matches)-1], ver, true, nil
	}
	_, fName := filepath.Split(matches[0])
	ver, _ := ParseVersion(fName)
	return matches[0], ver, true, nil
}

func CheckIsThereFileWithSupportedVersion(pattern string, minSup Version) error {
	_, fileVer, ok, err := FindFilesWithVersionsByPattern(pattern)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("file with this pattern not found")
	}
	if fileVer.Less(minSup) {
		return fmt.Errorf("file version %s is less than supported version %s", fileVer.String(), minSup.String())
	}
	return nil
}

func ReplaceVersionWithMask(path string) (string, error) {
	_, fName := filepath.Split(path)

	ver, err := ParseVersion(fName)
	if err != nil {
		return "", err
	}
	fNameOld := fName
	fName = strings.ReplaceAll(fName, ver.String(), "*")

	return strings.ReplaceAll(path, fNameOld, fName), nil
}

func (v *Version) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	ver, err := ParseVersion(s)
	if err != nil {
		return err
	}
	*v = ver
	return nil
}
