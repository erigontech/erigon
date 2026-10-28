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

// bscBootnodes are bsc-geth's MainnetBootnodes (params/bootnodes.go) and
// bscStaticPeers the StaticNodes of its mainnet.zip release asset, both on the
// devp2p listener port 30311.
var bscBootnodes = []string{
	"enode://433c8bfdf53a3e2268ccb1b829e47f629793291cbddf0c76ae626da802f90532251fc558e2e0d10d6725e759088439bf1cd4714716b03a259a35d4b2e4acfa7f@52.69.102.73:30311",
	"enode://571bee8fb902a625942f10a770ccf727ae2ba1bab2a2b64e121594a99c9437317f6166a395670a00b7d93647eacafe598b6bbcef15b40b6d1a10243865a3e80f@35.73.84.120:30311",
	"enode://fac42fb0ba082b7d1eebded216db42161163d42e4f52c9e47716946d64468a62da4ba0b1cac0df5e8bf1e5284861d757339751c33d51dfef318be5168803d0b5@18.203.152.54:30311",
	"enode://3063d1c9e1b824cfbb7c7b6abafa34faec6bb4e7e06941d218d760acdd7963b274278c5c3e63914bd6d1b58504c59ec5522c56f883baceb8538674b92da48a96@34.250.32.100:30311",
	"enode://ad78c64a4ade83692488aa42e4c94084516e555d3f340d9802c2bf106a3df8868bc46eae083d2de4018f40e8d9a9952c32a0943cd68855a9bc9fd07aac982a6d@34.204.214.24:30311",
	"enode://5db798deb67df75d073f8e2953dad283148133acb520625ea804c9c4ad09a35f13592a762d8f89056248f3889f6dcc33490c145774ea4ff2966982294909b37a@107.20.191.97:30311",
}

var bscStaticPeers = []string{
	"enode://551c8009f1d5bbfb1d64983eeb4591e51ad488565b96cdde7e40a207cfd6c8efa5b5a7fa88ed4e71229c988979e4c720891287ddd7d00ba114408a3ceb972ccb@34.245.70.169:30311",
	"enode://c637c90d6b9d1d0038788b163a749a7a86fed2e7d0d13e5dc920ab144bb432ed1e3e00b54c1a93cecba479037601ba9a5937a88fe0be949c651043473c0d1e5b@52.17.25.179:30311",
	"enode://bac6a548c7884270d53c3694c93ea43fa87ac1c7219f9f25c9d57f6a2fec9d75441bc4bad1e81d78c049a1c4daf3b1404e2bbb5cd9bf60c0f3a723bbaea110bc@54.171.201.213:30311",
	"enode://a232f92d1e76447b93306ece2f6a55ac70ca4633fae0938d71a100757eaf8526e6bbf720aa70cba1e6d186be17291ad1ee851a35596ec6caa2fdf135ce4b6b68@107.20.124.16:30311",
	"enode://accbc0a5af0af03e1ec3b5e80544bdceea48011a6928cd82d2c1a9c38b65fd48ec970ba17bd8c0b0ec21a28faec9efe1d1ce55134784b9207146e2f62d8932ba@54.162.32.1:30311",
	"enode://16c7e98f78017dafeaa4129647d1ec66b32ee9be5ec753708820b7363091ceb310f575e7abd9603005e0e34d7b3316c1a4b6c8c42d7f074ed2eb4d073f800a03@3.85.216.212:30311",
}

// chapelPeers are the enode URLs bsc-geth ships as `StaticNodes` in the
// testnet.zip release asset, on the devp2p listener port 30311. Chapel ships no
// discv4 bootnodes of its own, so these double as discv4 seeds in addition to
// being dialled persistently.
var chapelPeers = []string{
	"enode://db1e2c76e34f85b75fdc2460aad25a64947acc4adabb60b4c95f50c03066a4884f44f2d4d4c1607190712a0315681d30caa8a1c7d850e7aa643e29a6c1692739@52.199.214.252:30311",
	"enode://e5c4320eaa3357286cdde303df8b5b84f81013d86a72f91ecb2efc59b48a376bf16904d0a4e8ca44981c8d201bef439e1fb91c551d24aa39b65d930f03fc1823@52.51.80.128:30311",
	"enode://75601809401e4dedf6477fa9b74170d932b76aba0d1de1c19b27ff0a424ede294b5fc235af64f41dd4003a43793f63f321082b4de6d6a0588b5c84215f909af9@3.209.122.123:30311",
	"enode://665cf77ca26a8421cfe61a52ac312958308d4912e78ce8e0f61d6902e4494d4cc38f9b0dd1b23a427a7a5734e27e5d9729231426b06bb9c73b56a142f83f6b68@52.72.123.113:30311",
}
