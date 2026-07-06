// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// TestOnUsageErrorHandler verifies that the custom OnUsageError handler
// prints the error and help hint when called directly
func TestOnUsageErrorHandler(t *testing.T) {
	app := NewApp("test usage")
	app.Name = "test-app"

	// Capture stderr output
	var stderr bytes.Buffer
	app.ErrWriter = &stderr

	// Create a test context
	ctx := cli.NewContext(app, nil, nil)
	testErr := errors.New("flag parsing error")

	// Call the OnUsageError handler directly
	returnedErr := app.OnUsageError(ctx, testErr, false)

	// The handler should return cli.Exit with code 1
	var exitErr cli.ExitCoder
	require.ErrorAs(t, returnedErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())

	// Get the stderr output
	output := stderr.String()

	// Verify the error message is present
	require.Contains(t, output, "Error:")
	require.Contains(t, output, testErr.Error())

	// Verify the help hint is present
	require.Contains(t, output, "Run 'test-app --help' for usage.")
}

// TestExitErrHandler verifies that the custom ExitErrHandler is configured
func TestExitErrHandler(t *testing.T) {
	app := NewApp("test usage")
	require.NotNil(t, app.ExitErrHandler, "ExitErrHandler should be configured")
}

// TestHelpFlagStillWorks verifies that --help still prints full usage
func TestHelpFlagStillWorks(t *testing.T) {
	app := NewApp("test usage")
	app.Name = "test-app"
	app.Usage = "A test application"

	// Capture stdout (help goes to stdout by default)
	var stdout bytes.Buffer
	app.Writer = &stdout

	// Override ExitErrHandler to prevent actual exit during test
	app.ExitErrHandler = func(ctx *cli.Context, err error) {
		// Do nothing - don't exit in tests
	}

	app.Action = func(ctx *cli.Context) error {
		return nil
	}

	// Run the app with --help flag
	err := app.Run([]string{"test-app", "--help"})

	// The app should NOT return an error for --help
	require.NoError(t, err)

	// Get the output
	output := stdout.String()

	// Verify the full usage/help text IS present
	require.Contains(t, output, "test-app")
	require.Contains(t, output, "A test application")
	// Help output should contain standard sections
	require.True(t, strings.Contains(output, "USAGE") || strings.Contains(output, "NAME"))
}

// TestNewAppDefaults verifies that NewApp sets expected defaults
func TestNewAppDefaults(t *testing.T) {
	usage := "test usage string"

	app := NewApp(usage)

	// Verify basic properties are set
	require.NotNil(t, app)
	require.NotEmpty(t, app.Name)
	require.NotEmpty(t, app.Version)
	require.Equal(t, usage, app.Usage)

	// Verify OnUsageError handler is set
	require.NotNil(t, app.OnUsageError)

	// Verify ExitErrHandler is set
	require.NotNil(t, app.ExitErrHandler)
}
