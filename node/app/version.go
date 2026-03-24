package app

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	numbers  string = "0123456789"
	alphas          = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-"
	alphanum        = alphas + numbers
)

// Version represents either a numberic ro semantic version
type Version interface {
	Equals(o Version) bool
	CompareTo(o Version) int

	String() string

	Validate() error
}

func NewVersion(major interface{}, minor ...uint64) Version {
	var pmajor *uint64

	switch tmajor := major.(type) {
	case uint8:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case uint16:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case uint32:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case uint64:
		vmajor := tmajor
		pmajor = &vmajor
	case int8:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case int16:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case int32:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case int64:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case int:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	case uint:
		vmajor := uint64(tmajor)
		pmajor = &vmajor
	}

	var patch uint64

	if len(minor) > 1 {
		patch = minor[1]
	}

	return &SemanticVersion{*pmajor, minor[0], patch, nil, nil}
}

// NewPreReleaseVersion creates a new semantic version which includes pre release and build
//
//	info which will be appended with - & + respectively
func NewPreReleaseVersion(major, minor, patch uint64, preReleaseInfo, buildInfo []string) Version {

	preReleaseValues := []PreReleaseValue{}
	for _, prvalstr := range preReleaseInfo {
		prval, err := newPreReleaseValue(prvalstr)
		if err != nil {
			preReleaseValues = append(preReleaseValues, prval)
		}
	}

	return &SemanticVersion{major, minor, patch, preReleaseValues, buildInfo}
}

// SemanticVersion represents a semantic version as specified by http://semver.org/
type SemanticVersion struct {
	Major          uint64
	Minor          uint64
	Patch          uint64
	PreReleaseInfo []PreReleaseValue
	BuildInfo      []string
}

func NewSemanticVersion(major, minor, patch uint64) *SemanticVersion {
	return NewVersion(major, minor, patch).(*SemanticVersion)
}

// Version to string
func (v *SemanticVersion) String() string {
	b := make([]byte, 0, 5)
	b = strconv.AppendUint(b, v.Major, 10)
	b = append(b, '.')
	b = strconv.AppendUint(b, v.Minor, 10)
	b = append(b, '.')
	b = strconv.AppendUint(b, v.Patch, 10)

	if len(v.PreReleaseInfo) > 0 {
		b = append(b, '-')
		b = append(b, v.PreReleaseInfo[0].String()...)

		for _, pre := range v.PreReleaseInfo[1:] {
			b = append(b, '.')
			b = append(b, pre.String()...)
		}
	}

	if len(v.BuildInfo) > 0 {
		b = append(b, '+')
		b = append(b, v.BuildInfo[0]...)

		for _, build := range v.BuildInfo[1:] {
			b = append(b, '.')
			b = append(b, build...)
		}
	}

	return string(b)
}

// Equals checks if v is equal to o.
func (v *SemanticVersion) Equals(o Version) bool {
	return (v.CompareTo(o) == 0)
}

// CompareTo compares Versions v to o:
// -1 == v is less than o
// 0 == v is equal to o
// 1 == v is greater than o
func (v *SemanticVersion) CompareTo(other Version) int {
	o, ok := other.(*SemanticVersion)

	if !ok {
		return -1
	}

	if v == nil {
		if o == nil {
			return 0
		} else {
			return -1
		}
	} else {
		if o == nil {
			return 1
		}
	}

	if v.Major != o.Major {
		if v.Major > o.Major {
			return 1
		}
		return -1
	}
	if v.Minor != o.Minor {
		if v.Minor > o.Minor {
			return 1
		}
		return -1
	}
	if v.Patch != o.Patch {
		if v.Patch > o.Patch {
			return 1
		}
		return -1
	}

	// Quick comparison if a version has no prerelease versioning
	if len(v.PreReleaseInfo) == 0 && len(o.PreReleaseInfo) == 0 {
		return 0
	} else if len(v.PreReleaseInfo) == 0 && len(o.PreReleaseInfo) > 0 {
		return 1
	} else if len(v.PreReleaseInfo) > 0 && len(o.PreReleaseInfo) == 0 {
		return -1
	}

	i := 0
	for ; i < len(v.PreReleaseInfo) && i < len(o.PreReleaseInfo); i++ {
		if comp := v.PreReleaseInfo[i].CompareTo(o.PreReleaseInfo[i]); comp == 0 {
			continue
		} else if comp == 1 {
			return 1
		} else {
			return -1
		}
	}

	// If all pr versioning are the equal but one has further prversion, this one greater
	if i == len(v.PreReleaseInfo) && i == len(o.PreReleaseInfo) {
		return 0
	} else if i == len(v.PreReleaseInfo) && i < len(o.PreReleaseInfo) {
		return -1
	} else {
		return 1
	}

}

// Validate validates v and returns error in case
func (v *SemanticVersion) Validate() error {
	// Major, Minor, Patch already validated using uint64

	for _, pre := range v.PreReleaseInfo {
		if !pre.isNumeric { //Numeric prerelease versioning already uint64
			if len(pre.strValue) == 0 {
				return fmt.Errorf("prerelease can not be empty %q", pre.strValue)
			}
			if !containsOnly(pre.strValue, alphanum) {
				return fmt.Errorf("invalid character(s) found in prerelease %q", pre.strValue)
			}
		}
	}

	for _, build := range v.BuildInfo {
		if len(build) == 0 {
			return fmt.Errorf("build meta data can not be empty %q", build)
		}
		if !containsOnly(build, alphanum) {
			return fmt.Errorf("invalid character(s) found in build meta data %q", build)
		}
	}

	return nil
}

// ParseTolerant allows for certain version specifications that do not strictly adhere to semver
// specs to be parsed by this library. It does so by normalizing versioning before passing them to
// Parse(). It currently trims spaces, removes a "v" prefix, and adds a 0 patch number to versioning
// with only major and minor components specified
func ParseVersionTolerant(s string) (Version, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")

	// Split into major.minor.(patch+pr+meta)
	parts := strings.SplitN(s, ".", 3)
	if len(parts) < 3 {
		if strings.ContainsAny(parts[len(parts)-1], "+-") {
			return nil, errors.New("short version cannot contain PreRelease/Build meta data")
		}
		for len(parts) < 3 {
			parts = append(parts, "0")
		}
		s = strings.Join(parts, ".")
	}

	return ParseVersion(s)
}

// Parse parses version string and returns a validated Version or error
func ParseVersion(s string) (Version, error) {
	if len(s) == 0 {
		return nil, errors.New("version string empty")
	}

	if containsOnly(s, numbers) {
		_, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}

		return nil, errors.New("invalid version")
	}

	// Split into major.minor.(patch+pr+meta)
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return nil, errors.New("invalid version")
	}

	// Major
	if !containsOnly(parts[0], numbers) {
		return nil, fmt.Errorf("invalid character(s) found in major number %q", parts[0])
	}
	if hasLeadingZeroes(parts[0]) {
		return nil, fmt.Errorf("major number must not contain leading zeroes %q", parts[0])
	}
	major, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}

	// Minor
	if !containsOnly(parts[1], numbers) {
		return nil, fmt.Errorf("invalid character(s) found in minor number %q", parts[1])
	}
	if hasLeadingZeroes(parts[1]) {
		return nil, fmt.Errorf("minor number must not contain leading zeroes %q", parts[1])
	}
	minor, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}

	v := SemanticVersion{}
	v.Major = major
	v.Minor = minor

	var build, prerelease []string
	patchStr := parts[2]

	if buildIndex := strings.IndexRune(patchStr, '+'); buildIndex != -1 {
		build = strings.Split(patchStr[buildIndex+1:], ".")
		patchStr = patchStr[:buildIndex]
	}

	if preIndex := strings.IndexRune(patchStr, '-'); preIndex != -1 {
		prerelease = strings.Split(patchStr[preIndex+1:], ".")
		patchStr = patchStr[:preIndex]
	}

	if !containsOnly(patchStr, numbers) {
		return nil, fmt.Errorf("invalid character(s) found in patch number %q", patchStr)
	}
	if hasLeadingZeroes(patchStr) {
		return nil, fmt.Errorf("patch number must not contain leading zeroes %q", patchStr)
	}
	patch, err := strconv.ParseUint(patchStr, 10, 64)
	if err != nil {
		return nil, err
	}

	v.Patch = patch

	// Prerelease
	for _, prstr := range prerelease {
		parsedPR, err := newPreReleaseValue(prstr)
		if err != nil {
			return nil, err
		}
		v.PreReleaseInfo = append(v.PreReleaseInfo, parsedPR)
	}

	// Build meta data
	for _, str := range build {
		if len(str) == 0 {
			return nil, errors.New("build meta data is empty")
		}
		if !containsOnly(str, alphanum) {
			return nil, fmt.Errorf("invalid character(s) found in build meta data %q", str)
		}
		v.BuildInfo = append(v.BuildInfo, str)
	}

	return &v, nil
}

// MustParse is like Parse but panics if the version cannot be parsed.
func MustParseVersion(s string) Version {
	v, err := ParseVersion(s)
	if err != nil {
		panic(`semver: Parse(` + s + `): ` + err.Error())
	}
	return v
}

// PreReleaseValue represents PreRelease Version Info
type PreReleaseValue struct {
	strValue  string
	numValue  uint64
	isNumeric bool
}

func newPreReleaseValue(s string) (PreReleaseValue, error) {
	if len(s) == 0 {
		return PreReleaseValue{}, errors.New("prerelease is empty")
	}
	v := PreReleaseValue{}
	if containsOnly(s, numbers) {
		if hasLeadingZeroes(s) {
			return PreReleaseValue{}, fmt.Errorf("numeric PreRelease version must not contain leading zeroes %q", s)
		}
		num, err := strconv.ParseUint(s, 10, 64)

		// Might never be hit, but just in case
		if err != nil {
			return PreReleaseValue{}, err
		}
		v.numValue = num
		v.isNumeric = true
	} else if containsOnly(s, alphanum) {
		v.strValue = s
		v.isNumeric = false
	} else {
		return PreReleaseValue{}, fmt.Errorf("invalid character(s) found in prerelease %q", s)
	}
	return v, nil
}

// IsNumeric checks if prerelease-version is numeric
func (v PreReleaseValue) IsNumeric() bool {
	return v.isNumeric
}

// CompareTo compares two PreRelease Versions v and o:
// -1 == v is less than o
// 0 == v is equal to o
// 1 == v is greater than o
func (v PreReleaseValue) CompareTo(o PreReleaseValue) int {

	if v.isNumeric && !o.isNumeric {
		return -1
	} else if !v.isNumeric && o.isNumeric {
		return 1
	} else if v.isNumeric && o.isNumeric {
		if v.numValue == o.numValue {
			return 0
		} else if v.numValue > o.numValue {
			return 1
		} else {
			return -1
		}
	} else { // both are Alphas
		if v.strValue == o.strValue {
			return 0
		} else if v.strValue > o.strValue {
			return 1
		} else {
			return -1
		}
	}
}

// PreRelease version to string
func (v PreReleaseValue) String() string {
	if v.isNumeric {
		return strconv.FormatUint(v.numValue, 10)
	}
	return v.strValue
}

func newBuildValue(s string) (string, error) {

	if len(s) == 0 {
		return "", errors.New("build version is empty")
	}
	if !containsOnly(s, alphanum) {
		return "", fmt.Errorf("invalid character(s) found in build meta data %q", s)
	}
	return s, nil
}

func containsOnly(s string, set string) bool {
	return strings.IndexFunc(s, func(r rune) bool {
		return !strings.ContainsRune(set, r)
	}) == -1
}

func hasLeadingZeroes(s string) bool {
	return len(s) > 1 && s[0] == '0'
}
