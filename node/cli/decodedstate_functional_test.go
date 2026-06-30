package cli

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/ethconfig"
)

func newDefaultFlagsContext(t *testing.T, args ...string) *cli.Context {
	t.Helper()

	app := NewApp("decodedstate functional tests")
	app.Flags = DefaultFlags

	set := flag.NewFlagSet("decodedstate-functional", flag.ContinueOnError)
	for _, flg := range app.Flags {
		require.NoError(t, flg.Apply(set))
	}
	require.NoError(t, set.Parse(args))

	return cli.NewContext(app, set, nil)
}

func TestS_NODE_04_DefaultFlagsParseDecodedStateWhitelistModeFromCLI(t *testing.T) {
	whitelist := []string{
		"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
	}

	ctx := newDefaultFlagsContext(
		t,
		"--decoded.enabled",
		"--decoded.whitelist", whitelist[0],
		"--decoded.whitelist", whitelist[1],
	)

	require.True(t, ctx.Bool("decoded.enabled"))
	require.False(t, ctx.Bool("decoded.fullmode"), "decoded-state CLI must default to whitelist mode when full mode is not set")
	require.Equal(t, whitelist, ctx.StringSlice("decoded.whitelist"))
}

func TestS_NODE_05_ConfigFilePopulatesDecodedStateFullMode(t *testing.T) {
	ctx := newDefaultFlagsContext(t)

	cfgPath := filepath.Join(t.TempDir(), "decodedstate.yaml")
	cfg := []byte(`
decoded.enabled: true
decoded.fullmode: true
decoded.whitelist:
  - 0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
  - 0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
`)
	require.NoError(t, os.WriteFile(cfgPath, cfg, 0o644))

	require.NoError(t, SetFlagsFromConfigFile(ctx, cfgPath), "decoded-state activation must be configurable through normal node config files")
	require.True(t, ctx.Bool("decoded.enabled"))
	require.True(t, ctx.Bool("decoded.fullmode"))
	require.Equal(t,
		[]string{
			"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		},
		ctx.StringSlice("decoded.whitelist"),
	)
}

func TestDecodedStateConfigFileDoesNotOverrideExplicitCLIFlags(t *testing.T) {
	ctx := newDefaultFlagsContext(
		t,
		"--decoded.enabled",
		"--decoded.whitelist", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	)

	cfgPath := filepath.Join(t.TempDir(), "decodedstate.yaml")
	cfg := []byte(`
decoded.enabled: false
decoded.fullmode: true
decoded.whitelist:
  - 0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
`)
	require.NoError(t, os.WriteFile(cfgPath, cfg, 0o644))

	require.NoError(t, SetFlagsFromConfigFile(ctx, cfgPath))
	require.True(t, ctx.Bool("decoded.enabled"))
	require.True(t, ctx.Bool("decoded.fullmode"))
	require.Equal(t, []string{"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, ctx.StringSlice("decoded.whitelist"))
}

func TestS_NODE_09_DecodedFlagsAlterRuntimeEthConfig(t *testing.T) {
	ctxWithoutDecoded := newDefaultFlagsContext(t)
	ctxWithDecoded := newDefaultFlagsContext(
		t,
		"--decoded.enabled",
		"--decoded.fullmode",
		"--decoded.whitelist", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	)

	cfgWithoutDecoded := ethconfig.Defaults
	cfgWithDecoded := ethconfig.Defaults

	ApplyFlagsForEthConfig(ctxWithoutDecoded, &cfgWithoutDecoded, log.New())
	ApplyFlagsForEthConfig(ctxWithDecoded, &cfgWithDecoded, log.New())

	require.NotEqual(
		t,
		cfgWithoutDecoded,
		cfgWithDecoded,
		"decoded-state CLI flags must change runtime eth config so the execution pipeline can enable decoded-state collection",
	)
}
