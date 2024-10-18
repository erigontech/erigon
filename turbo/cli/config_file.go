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
