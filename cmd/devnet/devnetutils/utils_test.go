package devnetutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		require.EqualValues(t, got, testCase.expectedRes)
	}
}

func TestParseResponse(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	testCases := []struct {
		input    interface{}
		expected string
	}{
		{
			Person{
				Name: "Leonard",
				Age:  10,
			},
			`{"Name":"Leonard","Age":10}`,
		},
		{
			struct {
				Person struct {
					Name string
					Age  int
				}
				WorkID string
			}{
				Person: Person{
					Name: "Uzi",
					Age:  23,
				},
				WorkID: "123456",
			},
			`{"Person":{"Name":"Uzi","Age":23},"WorkID":"123456"}`,
		},
	}

	for _, testCase := range testCases {
		got, _ := ParseResponse(testCase.input)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestNamespaceAndSubMethodFromMethod(t *testing.T) {
	expectedError := fmt.Errorf("invalid string to split")

	testCases := []struct {
		method            string
		expectedNamespace string
		expectedSubMethod string
		shouldError       bool
		expectedError     error
	}{
		{
			"eth_logs",
			"eth",
			"logs",
			false,
			nil,
		},
		{
			"ethNewHeads",
			"",
			"",
			true,
			expectedError,
		},
		{
			"",
			"",
			"",
			true,
			expectedError,
		},
	}

	for _, testCase := range testCases {
		namespace, subMethod, err := NamespaceAndSubMethodFromMethod(testCase.method)
		require.EqualValues(t, testCase.expectedNamespace, namespace)
		require.EqualValues(t, testCase.expectedSubMethod, subMethod)
		require.EqualValues(t, testCase.expectedError, err)
		if testCase.shouldError {
			require.Errorf(t, testCase.expectedError, expectedError.Error())
		}
	}
}

func TestHexToInt(t *testing.T) {
	testCases := []struct {
		hexStr   string
		expected uint64
	}{
		{"0x0", 0},
		{"0x32424", 205860},
		{"0x200", 512},
		{"0x39", 57},
	}

	for _, testCase := range testCases {
		got := HexToInt(testCase.hexStr)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestGenerateTopic(t *testing.T) {
	testCases := []struct {
		signature string
		expected  string
	}{
		{"random string", "0x0d9d89437ff2d48ce95779dc9457bc48287b75a573eddbf50954efac5a97c4b9"},
		{"SubscriptionEvent()", "0x67abc7edb0ab50964ef0e90541d39366b9c69f6f714520f2ff4570059ee8ad80"},
		{"", "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"},
	}

	for _, testCase := range testCases {
		got := GenerateTopic(testCase.signature)
		require.EqualValues(t, testCase.expected, fmt.Sprintf("%s", got[0]))
	}
}
