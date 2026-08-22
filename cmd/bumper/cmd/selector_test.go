package cmd

import (
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestInitialSelection(t *testing.T) {
	domains := []string{"accounts", "storage", "logaddr"}
	types := map[string]string{
		"accounts": domainType,
		"storage":  domainType,
		"logaddr":  idxType,
	}
	domainExts := extCfgMap[domainType] // .kv .bt .kvi .kvei .vi .v
	idxExts := extCfgMap[idxType]       // .efi .ef

	tests := []struct {
		name           string
		includeDomains []string
		includeExts    []string
		excludeDomains []string
		excludeExts    []string
		wantPresent    []string
		wantAbsent     []string
	}{
		{
			name:        "no filters selects everything",
			wantPresent: concat([]string{"accounts", "storage", "logaddr"}, domainExts, idxExts),
		},
		{
			name:           "include-domains is a whitelist",
			includeDomains: []string{"accounts"},
			wantPresent:    []string{"accounts"},
			wantAbsent:     []string{"storage", "logaddr"},
		},
		{
			name:           "exclude-domains is a blacklist",
			excludeDomains: []string{"storage"},
			wantPresent:    []string{"accounts", "logaddr"},
			wantAbsent:     []string{"storage"},
		},
		{
			name:        "include-exts is a whitelist",
			includeExts: []string{".kv"},
			wantPresent: []string{"accounts", "storage", ".kv"},
			wantAbsent:  []string{".bt", ".kvi", ".kvei", ".vi", ".v", ".ef", ".efi"},
		},
		{
			name:        "include-exts whitelist selects idx exts too",
			includeExts: []string{".ef"},
			wantPresent: []string{"logaddr", ".ef"},
			wantAbsent:  []string{".efi", ".kv", ".bt"},
		},
		{
			name:        "exclude-exts is a blacklist",
			excludeExts: []string{".bt"},
			wantPresent: []string{".kv", ".kvi", ".kvei", ".vi", ".v", ".efi", ".ef"},
			wantAbsent:  []string{".bt"},
		},
		{
			name:        "include-exts overrides exclude-exts",
			includeExts: []string{".kv"},
			excludeExts: []string{".kv"},
			wantPresent: []string{".kv"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sel := initialSelection(domains, types, tc.includeDomains, tc.includeExts, tc.excludeDomains, tc.excludeExts)
			for _, k := range tc.wantPresent {
				if _, ok := sel[k]; !ok {
					t.Errorf("expected %q to be selected, but it was not", k)
				}
			}
			for _, k := range tc.wantAbsent {
				if _, ok := sel[k]; ok {
					t.Errorf("expected %q to NOT be selected, but it was", k)
				}
			}
		})
	}
}

// getNames must only surface domains that renameFiles can actually rename;
// renameFiles resolves each selected name via kv.String2Enum and aborts on the
// first failure, so a non-renamable name (e.g. a block-data field) breaks the
// whole run.
func TestGetNamesAreRenamable(t *testing.T) {
	res, domains := getNames(&statecfg.Schema)
	if len(domains) == 0 {
		t.Fatal("getNames returned no domains")
	}
	for _, d := range domains {
		if _, err := kv.String2Enum(d); err != nil {
			t.Errorf("getNames returned %q, which renameFiles cannot resolve: %v", d, err)
		}
		if res[d] == "" {
			t.Errorf("getNames returned %q with an empty type", d)
		}
	}
}

func concat(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
