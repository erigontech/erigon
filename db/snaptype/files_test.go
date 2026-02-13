package snaptype

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

const testHeadersEnum Enum = 2
const testBodiesEnum Enum = 3

func init() {
	RegisterType(testHeadersEnum, "headers", Versions{Current: Version{Major: 1, Minor: 2}, MinSupported: Version{Major: 1, Minor: 0}}, nil, nil, nil)
	RegisterType(testBodiesEnum, "bodies", Versions{Current: Version{Major: 1, Minor: 2}, MinSupported: Version{Major: 1, Minor: 0}}, nil, nil, nil)
}

func TestStateSeedable(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{
			name:     "valid seedable file",
			filename: "v12.13-accounts.100-164.efi",
			expected: true,
		},
		{
			name:     "seedable: we allow seed files of any size",
			filename: "v12.13-accounts.100-165.efi",
			expected: true,
		},
		{
			name:     "seedable: we allow seed files of any size",
			filename: "v12.13-accounts.100-101.efi",
			expected: true,
		},
		{
			name:     "invalid file name - regex not matching",
			filename: "invalid-file-name",
			expected: false,
		},
		{
			name:     "file with relative path prefix",
			filename: "history/v12.13-accounts.100-164.efi",
			expected: true,
		},
		{
			name:     "invalid file name - capital letters not allowed",
			filename: "v12.13-ACCC.100-164.efi",
			expected: false,
		},
		{
			name:     "block files are not state files",
			filename: "v1.2-headers.seg",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsStateFileSeedable(tc.filename)
			if result != tc.expected {
				t.Errorf("IsStateFileSeedable(%q) = %v; want %v", tc.filename, result, tc.expected)
			}
		})
	}
}

func TestParseFileNameHash(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		fileName string
		wantHash string
		wantFrom uint64
		wantTo   uint64
		wantType string
		wantOk   bool
	}{
		{
			name:     "V2 without hash",
			fileName: "v1.0-000000-001000-headers.seg",
			wantHash: "",
			wantFrom: 0,
			wantTo:   1_000_000,
			wantType: "headers",
			wantOk:   true,
		},
		{
			name:     "V2 with hash",
			fileName: "v1.0-000000-001000-headers.abc123def0.seg",
			wantHash: "abc123def0",
			wantFrom: 0,
			wantTo:   1_000_000,
			wantType: "headers",
			wantOk:   true,
		},
		{
			name:     "V3 state without hash",
			fileName: "v12.13-accounts.100-164.efi",
			wantHash: "",
			wantFrom: 100,
			wantTo:   164,
			wantType: "accounts",
			wantOk:   true,
		},
		{
			name:     "V3 state with hash",
			fileName: "v12.13-accounts.100-164.abc123def0.efi",
			wantHash: "abc123def0",
			wantFrom: 100,
			wantTo:   164,
			wantType: "accounts",
			wantOk:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fi, _, ok := ParseFileName(tc.dir, tc.fileName)
			if ok != tc.wantOk {
				t.Fatalf("ParseFileName(%q, %q) ok = %v, want %v", tc.dir, tc.fileName, ok, tc.wantOk)
			}
			if !ok {
				return
			}
			if fi.Hash != tc.wantHash {
				t.Errorf("Hash = %q, want %q", fi.Hash, tc.wantHash)
			}
			if fi.From != tc.wantFrom {
				t.Errorf("From = %d, want %d", fi.From, tc.wantFrom)
			}
			if fi.To != tc.wantTo {
				t.Errorf("To = %d, want %d", fi.To, tc.wantTo)
			}
			if fi.TypeString != tc.wantType {
				t.Errorf("TypeString = %q, want %q", fi.TypeString, tc.wantType)
			}
		})
	}
}

func TestFileNameWithHashFunc(t *testing.T) {
	ver := Version{Major: 1, Minor: 0}

	name := FileNameWithHash(ver, 0, 1_000_000, "headers", "")
	if want := "v1.0-000000-001000-headers"; name != want {
		t.Errorf("FileNameWithHash (no hash) = %q, want %q", name, want)
	}

	name = FileNameWithHash(ver, 0, 1_000_000, "headers", "abc123def0")
	if want := "v1.0-000000-001000-headers.abc123def0"; name != want {
		t.Errorf("FileNameWithHash (with hash) = %q, want %q", name, want)
	}
}

func TestSegmentFileNameWithHashFunc(t *testing.T) {
	ver := Version{Major: 1, Minor: 0}

	name := SegmentFileNameWithHash(ver, 0, 1_000_000, testHeadersEnum, "abc123def0")
	if want := "v1.0-000000-001000-headers.abc123def0.seg"; name != want {
		t.Errorf("got %q, want %q", name, want)
	}

	// Without hash should equal SegmentFileName
	name = SegmentFileNameWithHash(ver, 0, 1_000_000, testHeadersEnum, "")
	want := SegmentFileName(ver, 0, 1_000_000, testHeadersEnum)
	if name != want {
		t.Errorf("got %q, want %q", name, want)
	}
}

func TestFileInfoWithHash(t *testing.T) {
	fi, _, ok := ParseFileName("/snap", "v1.0-000000-001000-headers.seg")
	if !ok {
		t.Fatal("failed to parse")
	}

	// Add hash
	hashed := fi.WithHash("deadbeef01234567")
	if hashed.Hash != "deadbeef01234567" {
		t.Errorf("Hash = %q, want %q", hashed.Hash, "deadbeef01234567")
	}
	if want := "v1.0-000000-001000-headers.deadbeef01234567.seg"; hashed.Name() != want {
		t.Errorf("Name = %q, want %q", hashed.Name(), want)
	}
	if hashed.From != fi.From || hashed.To != fi.To || hashed.TypeString != fi.TypeString {
		t.Error("WithHash should preserve From, To, TypeString")
	}

	// Replace hash
	rehashed := hashed.WithHash("newcafebabe1234")
	if want := "v1.0-000000-001000-headers.newcafebabe1234.seg"; rehashed.Name() != want {
		t.Errorf("Name after rehash = %q, want %q", rehashed.Name(), want)
	}

	// Remove hash
	unhashed := hashed.WithHash("")
	if unhashed.Hash != "" {
		t.Errorf("Hash should be empty, got %q", unhashed.Hash)
	}
	if want := "v1.0-000000-001000-headers.seg"; unhashed.Name() != want {
		t.Errorf("Name after removing hash = %q, want %q", unhashed.Name(), want)
	}
}

func TestFileInfoWithHashV3(t *testing.T) {
	fi, _, ok := ParseFileName("/snap", "v12.13-accounts.100-164.efi")
	if !ok {
		t.Fatal("failed to parse")
	}

	hashed := fi.WithHash("abc123def0")
	if want := "v12.13-accounts.100-164.abc123def0.efi"; hashed.Name() != want {
		t.Errorf("Name = %q, want %q", hashed.Name(), want)
	}

	// Round-trip: parse the hashed name back
	fi2, _, ok := ParseFileName("/snap", hashed.Name())
	if !ok {
		t.Fatalf("failed to parse hashed name %q", hashed.Name())
	}
	if fi2.Hash != "abc123def0" {
		t.Errorf("Hash after round-trip = %q, want %q", fi2.Hash, "abc123def0")
	}
	if fi2.From != 100 || fi2.To != 164 {
		t.Errorf("From/To after round-trip = %d/%d, want 100/164", fi2.From, fi2.To)
	}
	if fi2.TypeString != "accounts" {
		t.Errorf("TypeString after round-trip = %q, want %q", fi2.TypeString, "accounts")
	}
}

func TestFileInfoAsStripsHash(t *testing.T) {
	fi, _, ok := ParseFileName("/snap", "v1.0-000000-001000-headers.abc123def0.seg")
	if !ok {
		t.Fatal("failed to parse")
	}

	bodiesType, typeOk := ParseFileType("bodies")
	if !typeOk {
		t.Fatal("bodies type not registered")
	}

	converted := fi.As(bodiesType)
	if converted.Hash != "" {
		t.Errorf("As() should strip hash (content-specific), got %q", converted.Hash)
	}
	if want := "v1.0-000000-001000-bodies.seg"; converted.Name() != want {
		t.Errorf("As() name = %q, want %q", converted.Name(), want)
	}
}

func TestMaskMatchesHashedFiles(t *testing.T) {
	mask := SegmentFileMask(0, 1_000_000, testHeadersEnum)

	// Should match unhashed
	matched, err := filepath.Match(mask, "v1.0-000000-001000-headers.seg")
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Errorf("mask %q should match unhashed file", mask)
	}

	// Should match hashed
	matched, err = filepath.Match(mask, "v1.0-000000-001000-headers.abc123def0.seg")
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Errorf("mask %q should match hashed file", mask)
	}
}

func TestApplyContentHash(t *testing.T) {
	dir := t.TempDir()

	// Create a fake .seg file with known content
	content := []byte("hello snapshot world")
	origName := "v1.0-000000-001000-headers.seg"
	origPath := filepath.Join(dir, origName)
	if err := os.WriteFile(origPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	fi, _, ok := ParseFileName(dir, origName)
	if !ok {
		t.Fatalf("failed to parse %q", origName)
	}

	result, err := ApplyContentHash(fi)
	if err != nil {
		t.Fatalf("ApplyContentHash: %v", err)
	}

	// Verify hash matches SHA256 of content
	h := sha256.Sum256(content)
	wantHash := hex.EncodeToString(h[:4])
	if result.Hash != wantHash {
		t.Errorf("Hash = %q, want %q", result.Hash, wantHash)
	}

	// Verify original file no longer exists
	if _, err := os.Stat(origPath); !os.IsNotExist(err) {
		t.Error("original file should have been renamed")
	}

	// Verify new file exists at result.Path
	if _, err := os.Stat(result.Path); err != nil {
		t.Errorf("new file should exist at %q: %v", result.Path, err)
	}

	// Verify the new filename contains the hash
	wantName := "v1.0-000000-001000-headers." + wantHash + ".seg"
	if result.Name() != wantName {
		t.Errorf("Name = %q, want %q", result.Name(), wantName)
	}

	// Verify round-trip parse
	fi2, _, ok := ParseFileName(dir, result.Name())
	if !ok {
		t.Fatalf("failed to parse hashed name %q", result.Name())
	}
	if fi2.Hash != wantHash {
		t.Errorf("round-trip Hash = %q, want %q", fi2.Hash, wantHash)
	}
}

func TestParseFileNameHashRoundTrip(t *testing.T) {
	ver := Version{Major: 1, Minor: 0}

	// Construct with hash, parse back, verify all fields
	name := SegmentFileNameWithHash(ver, 0, 1_000_000, testHeadersEnum, "cafebabe")
	fi, _, ok := ParseFileName("/snap", name)
	if !ok {
		t.Fatalf("failed to parse %q", name)
	}
	if fi.Hash != "cafebabe" {
		t.Errorf("Hash = %q, want %q", fi.Hash, "cafebabe")
	}
	if fi.From != 0 || fi.To != 1_000_000 {
		t.Errorf("From/To = %d/%d, want 0/1000000", fi.From, fi.To)
	}
	if fi.TypeString != "headers" {
		t.Errorf("TypeString = %q, want %q", fi.TypeString, "headers")
	}
	if fi.Ext != ".seg" {
		t.Errorf("Ext = %q, want %q", fi.Ext, ".seg")
	}
}
