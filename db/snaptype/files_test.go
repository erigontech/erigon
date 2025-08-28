package snaptype

import "testing"

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
