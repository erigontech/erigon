package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/gateway-fm/vectorized-poseidon-gold/src/vectorizedposeidongold"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/node"
)

func main() {
	defer func() {
		/*
			panicResult := recover()
			if panicResult == nil {
				return
			}

			log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
			os.Exit(1)
		*/
	}()

	app := erigonapp.MakeApp("cdk-erigon", runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) error {
	configFilePath := cliCtx.String(utils.ConfigFlag.Name)
	if configFilePath != "" {
		if err := setFlagsFromConfigFile(cliCtx, configFilePath); err != nil {
			return fmt.Errorf("failed setting config flags from yaml/toml file: %v", err)
		}
	}

	logging.SetupLoggerCtx("cdk-erigon", cliCtx, log.LvlInfo, log.LvlInfo, true)

	// initializing the node and providing the current git commit there
	log.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	log.Info("Poseidon hashing", "Accelerated", vectorizedposeidongold.UsingSimd || vectorizedposeidongold.UsingScalars)

	logger := log.New()
	nodeCfg := node.NewNodConfigUrfave(cliCtx, logger)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg, logger)

	ethNode, err := node.New(cliCtx.Context, nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return err
	}
	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
	return err
}

func setFlagsFromConfigFile(ctx *cli.Context, filePath string) error {
	cfg, err := fileConfig(filePath)
	if err != nil {
		return err
	}

	for key, value := range cfg {
		if ctx.IsSet(key) {
			continue
		}

		if err := setFlag(ctx, key, value); err != nil {
			return err
		}
	}

	return nil
}

func fileConfig(filePath string) (map[string]interface{}, error) {
	fileExtension := filepath.Ext(filePath)
	switch fileExtension {
	case ".yaml":
		return yamlConfig(filePath)
	case ".toml":
		return tomlConfig(filePath)
	default:
		return nil, errors.New("config files only accepted are .yaml and .toml")
	}
}

func yamlConfig(filePath string) (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func tomlConfig(filePath string) (map[string]interface{}, error) {
	cfg := make(map[string]interface{})

	tomlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	err = toml.Unmarshal(tomlFile, &cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func setFlag(ctx *cli.Context, key string, value interface{}) error {
	isSlice := reflect.ValueOf(value).Kind() == reflect.Slice
	if isSlice {
		return setMultiValueFlag(ctx, key, value)
	}
	return setSingleValueFlag(ctx, key, value)
}

func setMultiValueFlag(ctx *cli.Context, key string, value interface{}) error {
	sliceInterface := value.([]interface{})
	slice := make([]string, len(sliceInterface))
	for i, v := range sliceInterface {
		slice[i] = fmt.Sprintf("%v", v)
	}

	return setFlagInContext(ctx, key, strings.Join(slice, ","))
}

func setSingleValueFlag(ctx *cli.Context, key string, value interface{}) error {
	return setFlagInContext(ctx, key, fmt.Sprintf("%v", value))
}

func setFlagInContext(ctx *cli.Context, key, value string) error {
	if err := ctx.Set(key, value); err != nil {
		return handleFlagError(key, value, err)
	}
	return nil
}

func handleFlagError(key, value string, err error) error {
	if deprecatedFlag, found := erigoncli.DeprecatedFlags[key]; found {
		if deprecatedFlag == "" {
			return fmt.Errorf("failed setting %s flag: it is deprecated, remove it", key)
		}
		return fmt.Errorf("failed setting %s flag: it is deprecated, use %s instead", key, deprecatedFlag)
	}

	errUnknownFlag := fmt.Errorf("no such flag -%s", key)
	if err.Error() == errUnknownFlag.Error() {
		log.Warn("🚨 failed setting flag: unknown flag provided", "key", key, "value", value)
		return nil
	}

	return fmt.Errorf("failed setting %s flag with value=%s, error=%s", key, value, err)
}
