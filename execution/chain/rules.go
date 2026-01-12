// Copyright 2025 The Erigon Authors
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

<<<<<<<< HEAD:txnprovider/shutter/cmd/register.go
package cmd

import (
	"github.com/urfave/cli/v2"
)

func RegisterCmds(app *cli.App) {
	registerValidatorRegCheckCmd(app)
}
========
package chain

type RulesName string

const (
	AuRaRules   RulesName = "aura"
	EtHashRules RulesName = "ethash"
	CliqueRules RulesName = "clique"
	BorRules    RulesName = "bor"
)
>>>>>>>> main:execution/chain/rules.go
