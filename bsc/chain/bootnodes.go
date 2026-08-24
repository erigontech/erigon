// Copyright 2026 The Erigon Authors
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

package chain

// chapelPeers are the enode URLs bsc-geth ships as `StaticNodes` in the
// testnet.zip release asset, on the devp2p listener port 30311. Chapel
// publishes no bootnodes and no DNS node list, so these seed discv4 as well as
// being dialled persistently.
var chapelPeers = []string{
	"enode://db1e2c76e34f85b75fdc2460aad25a64947acc4adabb60b4c95f50c03066a4884f44f2d4d4c1607190712a0315681d30caa8a1c7d850e7aa643e29a6c1692739@52.199.214.252:30311",
	"enode://e5c4320eaa3357286cdde303df8b5b84f81013d86a72f91ecb2efc59b48a376bf16904d0a4e8ca44981c8d201bef439e1fb91c551d24aa39b65d930f03fc1823@52.51.80.128:30311",
	"enode://75601809401e4dedf6477fa9b74170d932b76aba0d1de1c19b27ff0a424ede294b5fc235af64f41dd4003a43793f63f321082b4de6d6a0588b5c84215f909af9@3.209.122.123:30311",
	"enode://665cf77ca26a8421cfe61a52ac312958308d4912e78ce8e0f61d6902e4494d4cc38f9b0dd1b23a427a7a5734e27e5d9729231426b06bb9c73b56a142f83f6b68@52.72.123.113:30311",
}
