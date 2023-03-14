/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package metrics

// Config contains the configuration for the metric collection.
type Config struct { //nolint:maligned
	Enabled          bool   `toml:",omitempty"`
	EnabledExpensive bool   `toml:",omitempty"`
	HTTP             string `toml:",omitempty"`
	Port             int    `toml:",omitempty"`
}

// DefaultConfig is the default config for metrics used in go-ethereum.
var DefaultConfig = Config{
	Enabled:          false,
	EnabledExpensive: false,
	HTTP:             "127.0.0.1",
	Port:             6060,
}
