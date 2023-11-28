package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/pelletier/go-toml"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

func SetFlagsFromConfigFile(ctx *cli.Context, filePath string) error {
	fileExtension := filepath.Ext(filePath)

	fileConfig := make(map[string]interface{})

	if fileExtension == ".yml" || fileExtension == ".yaml" {
		yamlFile, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(yamlFile, fileConfig)
		if err != nil {
			return err
		}
	} else if fileExtension == ".toml" {
		tomlFile, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		err = toml.Unmarshal(tomlFile, &fileConfig)
		if err != nil {
			return err
		}
	} else {
		return errors.New("config files only accepted are .yaml and .toml")
	}
	// sets global flags to value in yaml/toml file
	for key, value := range fileConfig {
		if !ctx.IsSet(key) {
			if reflect.ValueOf(value).Kind() == reflect.Slice {
				sliceInterface := value.([]interface{})
				s := make([]string, len(sliceInterface))
				for i, v := range sliceInterface {
					s[i] = fmt.Sprintf("%v", v)
				}
				err := ctx.Set(key, strings.Join(s, ","))
				if err != nil {
					return fmt.Errorf("failed setting %s flag with values=%s error=%s", key, s, err)
				}
			} else {
				err := ctx.Set(key, fmt.Sprintf("%v", value))
				if err != nil {
					return fmt.Errorf("failed setting %s flag with value=%v error=%s", key, value, err)

				}
			}
		}
	}

	return nil
}
