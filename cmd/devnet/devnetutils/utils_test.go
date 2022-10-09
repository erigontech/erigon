package devnetutils

import "testing"

func TestUniqueIDFromEnode(t *testing.T) {
	testCases := []struct {
		input       string
		expectedRes string
		shouldError bool
	}{
		{
			input:       "",
			expectedRes: "",
			shouldError: true,
		},
		{
			input:       "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380?discport=0",
			expectedRes: "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380",
			shouldError: false,
		},
		{
			input:       "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380discport=0",
			expectedRes: "",
			shouldError: true,
		},
	}

	for _, testCase := range testCases {
		got, err := UniqueIDFromEnode(testCase.input)
		if testCase.shouldError && err == nil {
			t.Errorf("expected error to happen, got no error")
		}
		if !testCase.shouldError && err != nil {
			t.Errorf("expected no error, got %s", err)
		}
		if got != testCase.expectedRes {
			t.Errorf("expected %s, got %s", testCase.expectedRes, got)
		}
	}
}
