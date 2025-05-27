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

package observer

import "strings"

func clientNameBlacklist() []string {
	return []string{
		// bor/v0.2.14-stable-9edb2836/linux-amd64/go1.17.7
		// https://polygon.technology
		"bor",

		// Cypher/v1.9.24-unstable-a7d8c0f9/linux-amd64/go1.11
		// unknown, but it's likely outdated since almost all nodes are running on go 1.11 (2018)
		"Cypher",

		// Ecoball/v1.0.2-stable-ac03aee-20211125/x86_64-linux-gnu/rustc1.52.1
		// https://ecoball.org
		"Ecoball",

		// egem/v1.1.4-titanus-9b056f56-20210808/linux-amd64/go1.15.13
		// https://egem.io
		"egem",

		// energi3/v3.1.1-stable/linux-amd64/go1.15.8
		// https://energi.world
		"energi3",

		// Gdbix/v1.5.3-fluxdbix-f6911ea5/linux/go1.8.3
		// https://www.arabianchain.org
		"Gdbix",

		// Gddm/v0.8.1-master-7155d9dd/linux-amd64/go1.9.2
		// unknown, but it's likely outdated since all nodes are running on go 1.9 (2017)
		"Gddm",

		// Gero/v1.1.3-dev-f8efb930/linux-amd64/go1.13.4
		// https://sero.cash
		"Gero",

		// Gesn/v0.3.13-stable-b6c12eb2/linux-amd64/go1.12.4
		// https://ethersocial.org
		"Gesn",

		// Gexp/v1.10.8-stable-1eb55798/linux-amd64/go1.17
		// https://expanse.tech
		"Gexp",

		// Gnekonium/v1.6.6-stable-820982d6/linux-amd64/go1.9.2
		// https://nekonium.github.io
		"Gnekonium",

		// go-corex/v1.0.0-rc.1-6197c8bf-1638348709/linux-amd64/go1.17.3
		// https://www.corexchain.io
		"go-corex",

		// go-opera/v1.1.0-rc.4-91951f74-1647353617/linux-amd64/go1.17.8
		// https://www.fantom.foundation
		"go-opera",

		// go-photon/v1.0.2-rc.5-32a52936-1646808549/linux-amd64/go1.17.8
		// https://github.com/TechPay-io/go-photon
		"go-photon",

		// GoChain/v4.0.2/linux-amd64/go1.17.3
		// https://gochain.io
		"GoChain",

		// gqdc/v1.5.2-stable-53b6a36d/linux-amd64/go1.13.4
		// https://quadrans.io
		"gqdc",

		// Gtsf/v1.2.1-stable-df201e7e/linux-amd64/go1.13.4
		// https://tsf-network.com
		"Gtsf",

		// Gubiq/v7.0.0-monoceros-c9009e89/linux-amd64/go1.17.6
		// https://ubiqsmart.com
		"Gubiq",

		// Gvns/v3.2.0-unstable/linux-amd64/go1.12.4
		// https://github.com/AMTcommunity/go-vnscoin
		"Gvns",

		// Moac/v2.1.5-stable-af7bea47/linux-amd64/go1.13.4
		// https://www.moac.io
		"Moac",

		// pchain/linux-amd64/go1.13.3
		// http://pchain.org
		"pchain",

		// Pirl/v1.9.12-v7-masternode-premium-lion-ea07aebf-20200407/linux-amd64/go1.13.6
		// https://pirl.io
		"Pirl",

		// Q-Client/v1.0.8-stable/Geth/v1.10.8-stable-850a0145/linux-amd64/go1.16.15
		// https://q.org
		"Q-Client",

		// qk_node/v1.10.16-stable-75ceb6c6-20220308/linux-amd64/go1.17.8
		// https://quarkblockchain.medium.com
		"qk_node",

		// Quai/v1.10.10-unstable-b1b52e79-20220226/linux-amd64/go1.17.7
		// https://www.qu.ai
		"Quai",

		// REOSC/v2.2.4-unstable-6bcba06-20190321/x86_64-linux-gnu/rustc1.37.0
		// https://www.reosc.io
		"REOSC",

		// ronin/v2.3.0-stable-f07cd8d1/linux-amd64/go1.15.5
		// https://wallet.roninchain.com
		"ronin",
	}
}

func IsClientIDBlacklisted(clientID string) bool {
	// some unknown clients return an empty string
	if clientID == "" {
		return true
	}
	for _, clientName := range clientNameBlacklist() {
		if strings.HasPrefix(clientID, clientName) {
			return true
		}
	}
	return false
}

func NameFromClientID(clientID string) string {
	parts := strings.SplitN(clientID, "/", 2)
	return parts[0]
}
