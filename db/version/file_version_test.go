package version

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParseVersion(t *testing.T) {
	type args struct {
		v string
	}
	tests := []struct {
		name    string
		args    args
		want    Version
		wantErr bool
	}{
		{
			"v1.0",
			args{v: "v1.0"},
			V1_0,
			false,
		},
		{
			"v1",
			args{v: "v1"},
			V1_0,
			false,
		},
		{
			"v1.0-008800-008900-bormilestones.seg",
			args{v: "v1.0-008800-008900-bormilestones.seg"},
			V1_0,
			false,
		},
		{
			"v1-008800-008900-bormilestones.seg",
			args{v: "v1-008800-008900-bormilestones.seg"},
			V1_0,
			false,
		},
		{
			"v2.0-008800-008900-bormilestones.seg",
			args{v: "v2.0-008800-008900-bormilestones.seg"},
			V2_0,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVersion(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindFilesWithVersionsByPattern_NoMatches(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "*-accounts.0-64.ef")

	path, ver, ok, err := FindFilesWithVersionsByPattern(pattern)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok == false when no matches, got true")
	}
	if path != "" {
		t.Fatalf("expected empty path, got %q", path)
	}
	if ver != (Version{}) {
		t.Fatalf("expected zero Version, got %+v", ver)
	}
}

func TestFindFilesWithVersionsByPattern_SingleMatch(t *testing.T) {
	dir := t.TempDir()

	file := filepath.Join(dir, "v1.0-accounts.0-64.ef")
	if err := touch(file); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	pattern := filepath.Join(dir, "*-accounts.0-64.ef")

	path, ver, ok, err := FindFilesWithVersionsByPattern(pattern)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !ok {
		t.Fatalf("expected ok == true, got false")
	}
	if path != file {
		t.Fatalf("expected path %q, got %q", file, path)
	}
	if ver.Major != 1 || ver.Minor != 0 {
		t.Fatalf("expected version 1.0, got %+v", ver)
	}
}

func TestFindFilesWithVersionsByPattern_MultipleMatches_ReturnsHighestVersion(t *testing.T) {
	dir := t.TempDir()

	files := []string{
		filepath.Join(dir, "v1.0-accounts.0-64.ef"),
		filepath.Join(dir, "v1.5-code.1408-1472.ef"),
		filepath.Join(dir, "v2.0-storage.1472-1536.ef"),
	}
	for _, f := range files {
		if err := touch(f); err != nil {
			t.Fatalf("failed to create file %q: %v", f, err)
		}
	}

	pattern := filepath.Join(dir, "*.ef")

	path, ver, ok, err := FindFilesWithVersionsByPattern(pattern)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !ok {
		t.Fatalf("expected ok == true, got false")
	}

	expectedPath := files[2] // v2.0-storage.1472-1536.ef
	if path != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, path)
	}
	if ver.Major != 2 || ver.Minor != 0 {
		t.Fatalf("expected version 2.0, got %+v", ver)
	}

	morePrecisePattern := filepath.Join(dir, "*-accounts.0-64.ef")
	path, ver, ok, err = FindFilesWithVersionsByPattern(morePrecisePattern)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !ok {
		t.Fatalf("expected ok == true, got false")
	}

	expectedPath = files[0] // v1.0-accounts.0-64.ef
	if path != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, path)
	}
	if ver.Major != 1 || ver.Minor != 0 {
		t.Fatalf("expected version 1.0, got %+v", ver)
	}
}

func TestFindFilesWithVersionsByPattern_InvalidPattern(t *testing.T) {
	// filepath.Glob returns an error for invalid patterns like "["
	_, _, ok, err := FindFilesWithVersionsByPattern("[")
	if err == nil {
		t.Fatalf("expected error for invalid pattern, got nil")
	}
	if ok {
		t.Fatalf("expected ok == false on error, got true")
	}
}

// touch creates an empty file (like the `touch` shell command).
func touch(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func TestMatchVersionedFile(t *testing.T) {
	dir := t.TempDir()

	// Create test files with different versions
	if err := touch(filepath.Join(dir, "v1.0-accounts.0-1.kv")); err != nil {
		t.Fatal(err)
	}
	if err := touch(filepath.Join(dir, "v1.1-accounts.0-1.kv")); err != nil {
		t.Fatal(err)
	}
	if err := touch(filepath.Join(dir, "v1.0-storage.0-1.kv")); err != nil {
		t.Fatal(err)
	}

	// Simulate pre-scanned directory entries (just filenames)
	dirEntries := []string{
		"v1.0-accounts.0-1.kv",
		"v1.1-accounts.0-1.kv",
		"v1.0-storage.0-1.kv",
	}

	// Test 1: Find highest version of accounts
	path, ver, ok, err := MatchVersionedFile("*-accounts.0-1.kv", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 1 || ver.Minor != 1 {
		t.Fatalf("expected version 1.1, got %+v", ver)
	}
	if filepath.Base(path) != "v1.1-accounts.0-1.kv" {
		t.Fatalf("expected v1.1-accounts.0-1.kv, got %s", filepath.Base(path))
	}

	// Test 2: Find storage (only one version)
	path, ver, ok, err = MatchVersionedFile("*-storage.0-1.kv", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 1 || ver.Minor != 0 {
		t.Fatalf("expected version 1.0, got %+v", ver)
	}
	if filepath.Base(path) != "v1.0-storage.0-1.kv" {
		t.Fatalf("expected v1.0-storage.0-1.kv, got %s", filepath.Base(path))
	}

	// Test 3: No match
	_, _, ok, err = MatchVersionedFile("*-code.0-1.kv", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok == false for no matches")
	}
}

func TestMatchVersionedFile_MultipleVersions(t *testing.T) {
	dir := t.TempDir()

	// Test with many versions
	dirEntries := []string{
		"v1.0-test.0-1.ef",
		"v1.1-test.0-1.ef",
		"v1.2-test.0-1.ef",
		"v2.0-test.0-1.ef",
		"v2.1-test.0-1.ef",
	}

	path, ver, ok, err := MatchVersionedFile("*-test.0-1.ef", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 2 || ver.Minor != 1 {
		t.Fatalf("expected version 2.1, got %+v", ver)
	}
	if filepath.Base(path) != "v2.1-test.0-1.ef" {
		t.Fatalf("expected v2.1-test.0-1.ef, got %s", filepath.Base(path))
	}
}

func TestMatchVersionedFile_AccessorFiles(t *testing.T) {
	dir := t.TempDir()

	// Test with accessor file extensions (.vi, .efi, .kvi, etc.)
	dirEntries := []string{
		"v1.0-accounts.0-1.vi",
		"v1.1-accounts.0-1.vi",
		"v1.0-accounts.1-2.vi",
		"v1.0-storage.0-1.efi",
		"v2.0-storage.0-1.efi",
	}

	// Test .vi files
	path, ver, ok, err := MatchVersionedFile("*-accounts.0-1.vi", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 1 || ver.Minor != 1 {
		t.Fatalf("expected version 1.1, got %+v", ver)
	}
	if filepath.Base(path) != "v1.1-accounts.0-1.vi" {
		t.Fatalf("expected v1.1-accounts.0-1.vi, got %s", filepath.Base(path))
	}

	// Test .efi files
	path, ver, ok, err = MatchVersionedFile("*-storage.0-1.efi", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 2 || ver.Minor != 0 {
		t.Fatalf("expected version 2.0, got %+v", ver)
	}
	if filepath.Base(path) != "v2.0-storage.0-1.efi" {
		t.Fatalf("expected v2.0-storage.0-1.efi, got %s", filepath.Base(path))
	}

	// Test different step range
	path, ver, ok, err = MatchVersionedFile("*-accounts.1-2.vi", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 1 || ver.Minor != 0 {
		t.Fatalf("expected version 1.0, got %+v", ver)
	}
	if filepath.Base(path) != "v1.0-accounts.1-2.vi" {
		t.Fatalf("expected v1.0-accounts.1-2.vi, got %s", filepath.Base(path))
	}
}

func TestMatchVersionedFile_EmptyList(t *testing.T) {
	dir := t.TempDir()

	// Test with empty list
	_, _, ok, err := MatchVersionedFile("*-accounts.0-1.kv", []string{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok == false for empty list")
	}
}

func TestMatchVersionedFile_InvalidPattern(t *testing.T) {
	dir := t.TempDir()
	dirEntries := []string{"v1.0-test.kv"}

	// Invalid pattern with unmatched bracket
	_, _, _, err := MatchVersionedFile("[invalid", dirEntries, dir)
	if err == nil {
		t.Fatal("expected error for invalid pattern")
	}
}

// TestMatchVersionedFile_DifferentSegAndIdxNames tests the case where segment files
// have different base names than their index files (e.g., blobsidecars.seg has blocksidecars.idx)
func TestMatchVersionedFile_DifferentSegAndIdxNames(t *testing.T) {
	dir := t.TempDir()

	// Simulate directory with blobsidecars.seg and blocksidecars.idx (different base names)
	dirEntries := []string{
		"v1.1-000000-000064-blobsidecars.seg",
		"v1.1-000000-000064-blocksidecars.idx",
		"v1.0-000000-000064-blocksidecars.idx", // older version
		"v1.1-000064-000128-blobsidecars.seg",
		"v1.1-000064-000128-blocksidecars.idx",
	}

	// Search for blocksidecars.idx should find it despite blobsidecars.seg being present
	path, ver, ok, err := MatchVersionedFile("*-000000-000064-blocksidecars.idx", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	// Should find the highest version (v1.1)
	if ver.Major != 1 || ver.Minor != 1 {
		t.Fatalf("expected version 1.1, got %+v", ver)
	}
	if filepath.Base(path) != "v1.1-000000-000064-blocksidecars.idx" {
		t.Fatalf("expected v1.1-000000-000064-blocksidecars.idx, got %s", filepath.Base(path))
	}

	// Search for blobsidecars.seg should find it
	path, ver, ok, err = MatchVersionedFile("*-000000-000064-blobsidecars.seg", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok == true")
	}
	if ver.Major != 1 || ver.Minor != 1 {
		t.Fatalf("expected version 1.1, got %+v", ver)
	}
	if filepath.Base(path) != "v1.1-000000-000064-blobsidecars.seg" {
		t.Fatalf("expected v1.1-000000-000064-blobsidecars.seg, got %s", filepath.Base(path))
	}

	// Search for non-existent beaconblocks.idx should return not found
	_, _, ok, err = MatchVersionedFile("*-000000-000064-beaconblocks.idx", dirEntries, dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok == false for non-existent file type")
	}
}

func BenchmarkMatchVersionedFile(b *testing.B) {
	// Simulate a large directory with thousands of snapshot files (realistic scenario)
	dirEntries := make([]string, 0, 2000)
	for i := 0; i < 500; i++ {
		dirEntries = append(dirEntries,
			fmt.Sprintf("v1.0-accounts.%d-%d.kv", i, i+1),
			fmt.Sprintf("v1.0-storage.%d-%d.kv", i, i+1),
			fmt.Sprintf("v1.0-accounts.%d-%d.kvi", i, i+1),
			fmt.Sprintf("v1.0-storage.%d-%d.kvi", i, i+1),
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = MatchVersionedFile("*-accounts.0-1.kvi", dirEntries, "/tmp")
	}
}
