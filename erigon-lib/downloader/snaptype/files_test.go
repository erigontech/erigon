package snaptype

import "testing"

func TestE3Seedable(t *testing.T) {
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
			name:     "non seedable due to wrong diff",
			filename: "v12.13-accounts.100-165.efi",
			expected: false,
		},
		{
			name:     "invalid file name - regex not matching",
			filename: "invalid-file-name",
			expected: false,
		},
		{
			name:     "file with path prefix",
			filename: "history/v12.13-accounts.100-164.efi",
			expected: true,
		},
		{
			name:     "invalid branch name",
			filename: "v12.13-ACCC.100-164.efi",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := E3Seedable(tc.filename)
			if result != tc.expected {
				t.Errorf("E3Seedable(%q) = %v; want %v", tc.filename, result, tc.expected)
			}
		})
	}
}
