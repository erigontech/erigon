/*
   Copyright 2022 Erigon-Lightclient contributors
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

package main

import (
	"context"

	"github.com/alecthomas/kong"
)

type Context struct {
	context.Context
	kctx *kong.Context
}

func main() {
	ctx := kong.Parse(&CLI)
	// Call the Run() method of the selected parsed command.
	err := ctx.Run(&Context{
		kctx:    ctx,
		Context: context.TODO(),
	})
	ctx.FatalIfErrorf(err)
}
