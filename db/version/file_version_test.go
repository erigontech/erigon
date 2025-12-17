package version

import (
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
